import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.response.*
import io.ktor.server.request.*
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
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.Connection
import com.rabbitmq.client.Channel
import java.net.HttpURLConnection
import java.net.URL
import java.util.UUID

@Serializable
data class Playlist(
    val id: Int,
    val name: String,
    val tracks: List<Int> = emptyList(),
    val owner: String = ""
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

private val logger = LoggerFactory.getLogger("PlaylistService")
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
        conn.outputStream.use { /* пустое тело */ }
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
        call.respond(HttpStatusCode.Unauthorized, ApiResponse("playlist-service", "Missing or invalid Authorization header"))
        return null
    }
    val token = hdr.removePrefix("Bearer ").trim()
    val user = validateTokenViaAuthService(token, requestId) ?: run {
        logger.warn("Invalid or expired token")
        call.respond(HttpStatusCode.Unauthorized, ApiResponse("playlist-service", "Invalid or expired token"))
        return null
    }
    if (requiredRole != null && !user.roles.contains(requiredRole)) {
        logger.warn("Insufficient permissions for user=${user.username}, required role=$requiredRole")
        call.respond(HttpStatusCode.Forbidden, ApiResponse("playlist-service", "Insufficient permissions"))
        return null
    }

    MDC.put("user", user.username)
    return user
}
// ---------- /АВТОРИЗАЦИЯ ----------

// RabbitMQ
class RabbitMQPublisher {
    private val logger = LoggerFactory.getLogger(RabbitMQPublisher::class.java)
    private var connection: Connection? = null
    private var channel: Channel? = null

    companion object {
        const val EXCHANGE_NAME = "music_events"
        const val PLAYLIST_QUEUE = "playlist_events"
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
            channel?.queueDeclare(PLAYLIST_QUEUE, true, false, false, null)
            channel?.queueBind(PLAYLIST_QUEUE, EXCHANGE_NAME, "playlist.#")
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
    }
}

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
            route("/api/playlists") {

                get {
                    val user = authenticateOrReject(call, requiredRole = "USER") ?: return@get
                    logger.info("Fetching all playlists for user=${user.username}")

                    val playlists = listOf(
                        Playlist(1, "My Rock", listOf(1, 3), owner = user.username),
                        Playlist(2, "Chill", listOf(2), owner = user.username)
                    )

                    val eventData = buildJsonObject {
                        put("count", playlists.size)
                        put("user", user.username)
                    }
                    rabbitmq.publishEvent(
                        "playlist.list",
                        EventMessage(
                            eventType = "PLAYLISTS_LISTED",
                            service = "playlist-service",
                            timestamp = System.currentTimeMillis(),
                            data = eventData
                        )
                    )

                    logger.info("Returning ${playlists.size} playlists to user=${user.username}")
                    call.respond(
                        ApiResponse(
                            "playlist-service",
                            "Список плейлистов пользователя",
                            Json.encodeToJsonElement(playlists)
                        )
                    )
                }

                get("/{id}") {
                    val user = authenticateOrReject(call, requiredRole = "USER") ?: return@get

                    val id = call.parameters["id"]?.toIntOrNull()
                    if (id == null) {
                        logger.warn("Invalid playlist ID requested")
                        call.respond(HttpStatusCode.BadRequest, ApiResponse("playlist-service", "Некорректный ID плейлиста"))
                        return@get
                    }

                    logger.info("Fetching playlist id=$id for user=${user.username}")
                    val pl = Playlist(id, "List #$id", listOf(1, 2, 3), owner = user.username)

                    val eventData = buildJsonObject {
                        put("playlistId", id)
                        put("user", user.username)
                    }
                    rabbitmq.publishEvent(
                        "playlist.view",
                        EventMessage(
                            eventType = "PLAYLIST_VIEWED",
                            service = "playlist-service",
                            timestamp = System.currentTimeMillis(),
                            data = eventData
                        )
                    )

                    call.respond(
                        ApiResponse(
                            "playlist-service",
                            "Информация о плейлисте",
                            Json.encodeToJsonElement(pl)
                        )
                    )
                }

                post {
                    val user = authenticateOrReject(call, requiredRole = "USER") ?: return@post
                    logger.info("Creating new playlist for user=${user.username}")

                    val req = call.receive<Playlist>()
                    val created = req.copy(id = (1000..9999).random(), owner = user.username)

                    val eventData = buildJsonObject {
                        put("playlistId", created.id)
                        put("user", user.username)
                    }
                    rabbitmq.publishEvent(
                        "playlist.created",
                        EventMessage(
                            eventType = "PLAYLIST_CREATED",
                            service = "playlist-service",
                            timestamp = System.currentTimeMillis(),
                            data = eventData
                        )
                    )

                    logger.info("Playlist created successfully id=${created.id} for user=${user.username}")
                    call.respond(
                        HttpStatusCode.Created,
                        ApiResponse(
                            "playlist-service",
                            "Плейлист создан",
                            Json.encodeToJsonElement(created)
                        )
                    )
                }
            }
        }
    }.start(wait = true)
}