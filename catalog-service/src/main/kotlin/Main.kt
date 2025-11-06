import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.plugins.contentnegotiation.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.encodeToString
import kotlinx.serialization.decodeFromString
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.Connection
import com.rabbitmq.client.Channel
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.net.HttpURLConnection
import java.net.URL
import java.util.UUID

@Serializable
data class Track(
    val id: Int,
    val title: String,
    val artist: String,
    val album: String,
    val duration: Int,
    val genre: String
)

@Serializable
data class Album(
    val id: Int,
    val title: String,
    val artist: String,
    val year: Int,
    val tracksCount: Int
)

@Serializable
data class Artist(
    val id: Int,
    val name: String,
    val genre: String,
    val albumsCount: Int
)

@Serializable
data class ApiResponse(
    val service: String,
    val message: String,
    val data: JsonElement? = null
)

@Serializable
data class EventMessage(
    val eventType: String,
    val service: String,
    val timestamp: Long,
    val data: JsonElement
)

// ---------- АВТОРИЗАЦИЯ ----------
@Serializable
data class AuthUser(
    val id: String,
    val username: String,
    val roles: List<String> = emptyList()
)

private val logger = LoggerFactory.getLogger("CatalogService")
private val jsonAuth = Json { ignoreUnknownKeys = true }

private fun validateTokenViaAuthService(token: String, requestId: String): AuthUser? {
    val urlStr = System.getenv("AUTH_VALIDATE_URL")
        ?: "http://auth-service:8080/api/auth/validate"
    val url = URL(urlStr)

    logger.info("Validating token via auth-service")

    val conn = (url.openConnection() as HttpURLConnection).apply {
        requestMethod = "POST"
        doOutput = true
        setFixedLengthStreamingMode(0)
        setRequestProperty("Authorization", "Bearer $token")
        setRequestProperty("X-Request-ID", requestId)  // Передаем Request ID
        connectTimeout = 8000
        readTimeout = 8000
        instanceFollowRedirects = false
    }

    return try {
        conn.outputStream.use { /* 0 байт */ }

        val code = conn.responseCode
        val stream = if (code in 200..299) conn.inputStream else conn.errorStream

        if (stream != null) {
            val body = stream.bufferedReader().use { it.readText() }
            if (code == 200) {
                val user = jsonAuth.decodeFromString<AuthUser>(body)
                logger.info("Token validated successfully for user=${user.username}")
                user
            } else {
                logger.warn("Token validation failed with status=$code")
                null
            }
        } else {
            logger.warn("Empty response from auth-service")
            null
        }
    } catch (ex: Exception) {
        logger.error("Error validating token: ${ex.message}", ex)
        null
    } finally {
        conn.disconnect()
    }
}

private suspend fun authenticateOrReject(call: ApplicationCall, requiredRole: String? = null): AuthUser? {
    val requestId = MDC.get("request_id")
    val hdr = call.request.headers[HttpHeaders.Authorization]

    if (hdr.isNullOrBlank() || !hdr.startsWith("Bearer ")) {
        logger.warn("Missing or invalid Authorization header")
        call.respond(HttpStatusCode.Unauthorized, ApiResponse("catalog-service", "Missing or invalid Authorization header"))
        return null
    }
    val token = hdr.removePrefix("Bearer ").trim()
    val user = validateTokenViaAuthService(token, requestId)
    if (user == null) {
        logger.warn("Invalid or expired token")
        call.respond(HttpStatusCode.Unauthorized, ApiResponse("catalog-service", "Invalid or expired token"))
        return null
    }
    if (requiredRole != null && !user.roles.contains(requiredRole)) {
        logger.warn("Insufficient permissions for user=${user.username}, required role=$requiredRole")
        call.respond(HttpStatusCode.Forbidden, ApiResponse("catalog-service", "Insufficient permissions"))
        return null
    }

    MDC.put("user", user.username)
    return user
}
// ---------- /АВТОРИЗАЦИЯ ----------

// RabbitMQ Helper
class RabbitMQPublisher {
    private val logger = LoggerFactory.getLogger(RabbitMQPublisher::class.java)
    private var connection: Connection? = null
    private var channel: Channel? = null

    companion object {
        const val EXCHANGE_NAME = "music_events"
        const val CATALOG_QUEUE = "catalog_events"
    }

    fun connect() {
        try {
            val factory = ConnectionFactory().apply {
                host = System.getenv("RABBITMQ_HOST") ?: "rabbitmq"
                port = (System.getenv("RABBITMQ_PORT") ?: "5672").toInt()
                username = System.getenv("RABBITMQ_USER") ?: "admin"
                password = System.getenv("RABBITMQ_PASS") ?: "admin123"
            }

            connection = factory.newConnection()
            channel = connection?.createChannel()

            channel?.exchangeDeclare(EXCHANGE_NAME, "topic", true)
            channel?.queueDeclare(CATALOG_QUEUE, true, false, false, null)
            channel?.queueBind(CATALOG_QUEUE, EXCHANGE_NAME, "catalog.#")

            logger.info("✅ Connected to RabbitMQ: ${factory.host}:${factory.port}")
        } catch (e: Exception) {
            logger.error("❌ Failed to connect to RabbitMQ: ${e.message}")
        }
    }

    fun publishEvent(routingKey: String, event: EventMessage) {
        try {
            val message = Json.encodeToString(EventMessage.serializer(), event)
            channel?.basicPublish(EXCHANGE_NAME, routingKey, null, message.toByteArray())
            logger.info("Published event: $routingKey -> ${event.eventType}")
        } catch (e: Exception) {
            logger.error("❌ Failed to publish event: ${e.message}")
        }
    }

    fun close() {
        channel?.close()
        connection?.close()
        logger.info("Disconnected from RabbitMQ")
    }
}

inline fun <reified T> createResponse(service: String, message: String, data: T): ApiResponse =
    ApiResponse(service = service, message = message, data = Json.encodeToJsonElement(data))

// Интерцептор для извлечения Request ID
fun Application.configureRequestIdInterceptor() {
    intercept(ApplicationCallPipeline.Setup) {
        val requestId = call.request.headers["X-Request-ID"] ?: UUID.randomUUID().toString()
        MDC.put("request_id", requestId)

        logger.info("Incoming request: method=${call.request.local.method}, uri=${call.request.local.uri}")

        try {
            proceed()
        } finally {
            MDC.clear()
        }
    }
}

fun main() {
    val rabbitmq = RabbitMQPublisher()

    rabbitmq.connect()
    Runtime.getRuntime().addShutdownHook(Thread { rabbitmq.close() })

    embeddedServer(Netty, port = 8080, host = "0.0.0.0") {
        install(ContentNegotiation) { json() }
        configureRequestIdInterceptor()

        routing {
            // Треки
            route("/api/catalog/tracks") {

                get {
                    val user = authenticateOrReject(call, requiredRole = "USER") ?: return@get
                    logger.info("Fetching all tracks for user=${user.username}")

                    val tracks = listOf(
                        Track(1, "Bohemian Rhapsody", "Queen", "A Night at the Opera", 354, "Rock"),
                        Track(2, "Imagine", "John Lennon", "Imagine", 183, "Pop"),
                        Track(3, "Smells Like Teen Spirit", "Nirvana", "Nevermind", 301, "Grunge")
                    )

                    val eventData = buildJsonObject {
                        put("count", tracks.size)
                        put("user", user.username)
                    }
                    rabbitmq.publishEvent(
                        "catalog.tracks.list",
                        EventMessage(
                            eventType = "TRACKS_LISTED",
                            service = "catalog-service",
                            timestamp = System.currentTimeMillis(),
                            data = eventData
                        )
                    )

                    logger.info("Returning ${tracks.size} tracks to user=${user.username}")
                    call.respond(createResponse("catalog-service", "Список всех треков", tracks))
                }

                get("/{id}") {
                    val user = authenticateOrReject(call, requiredRole = "USER") ?: return@get
                    val id = call.parameters["id"]?.toIntOrNull()
                    if (id == null) {
                        logger.warn("Invalid track ID requested")
                        call.respond(HttpStatusCode.BadRequest, ApiResponse("catalog-service", "Некорректный ID трека"))
                        return@get
                    }

                    logger.info("Fetching track id=$id for user=${user.username}")
                    val track = Track(id, "Track #$id", "Artist", "Album", 180, "Rock")

                    rabbitmq.publishEvent(
                        "catalog.tracks.view",
                        EventMessage(
                            eventType = "TRACK_VIEWED",
                            service = "catalog-service",
                            timestamp = System.currentTimeMillis(),
                            data = Json.encodeToJsonElement(track)
                        )
                    )

                    call.respond(createResponse("catalog-service", "Информация о треке", track))
                }

                get("/search") {
                    val user = authenticateOrReject(call, requiredRole = "USER") ?: return@get
                    val query = call.request.queryParameters["q"] ?: ""
                    logger.info("Searching tracks with query='$query' for user=${user.username}")

                    val result = listOf(Track(99, query, "Artist", "Album", 200, "Pop"))

                    val eventData = buildJsonObject {
                        put("q", query)
                        put("user", user.username)
                    }
                    rabbitmq.publishEvent(
                        "catalog.tracks.search",
                        EventMessage(
                            eventType = "TRACKS_SEARCHED",
                            service = "catalog-service",
                            timestamp = System.currentTimeMillis(),
                            data = eventData
                        )
                    )

                    call.respond(createResponse("catalog-service", "Результаты поиска", result))
                }
            }
        }
    }.start(wait = true)
}