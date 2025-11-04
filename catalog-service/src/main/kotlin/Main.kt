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
import java.net.HttpURLConnection
import java.net.URL

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

private val jsonAuth = Json { ignoreUnknownKeys = true }

private fun validateTokenViaAuthService(token: String): AuthUser? {
    val urlStr = System.getenv("AUTH_VALIDATE_URL")
        ?: "http://auth-service:8080/api/auth/validate"   // правильный путь по умолчанию
    val url = URL(urlStr)

    println("🔍 [AuthValidator] Проверка токена через $urlStr")

    val conn = (url.openConnection() as HttpURLConnection).apply {
        // validate — POST без тела
        requestMethod = "POST"
        doOutput = true
        setFixedLengthStreamingMode(0)
        setRequestProperty("Authorization", "Bearer $token")
        connectTimeout = 8000
        readTimeout = 8000
        instanceFollowRedirects = false
    }

    return try {
        // пустое тело
        conn.outputStream.use { /* 0 байт */ }

        val code = conn.responseCode
        val stream = if (code in 200..299) conn.inputStream else conn.errorStream
        println("ℹ️ [AuthValidator] Ответ от auth-service: HTTP $code")

        if (stream != null) {
            val body = stream.bufferedReader().use { it.readText() }
            println("📦 [AuthValidator] Тело ответа: $body")
            if (code == 200) {
                val user = jsonAuth.decodeFromString<AuthUser>(body)
                println("✅ [AuthValidator] Успешная валидация токена для пользователя: ${user.username}")
                user
            } else {
                println("⚠️ [AuthValidator] Ошибка валидации токена: $body")
                null
            }
        } else {
            println("⚠️ [AuthValidator] Пустой ответ от auth-service")
            null
        }
    } catch (ex: Exception) {
        println("❌ [AuthValidator] Ошибка при обращении к auth-service: ${ex.message}")
        ex.printStackTrace()
        null
    } finally {
        conn.disconnect()
        println("🔚 [AuthValidator] Подключение закрыто\n")
    }
}

private suspend fun authenticateOrReject(call: ApplicationCall, requiredRole: String? = null): AuthUser? {
    val hdr = call.request.headers[HttpHeaders.Authorization]
    if (hdr.isNullOrBlank() || !hdr.startsWith("Bearer ")) {
        call.respond(HttpStatusCode.Unauthorized, ApiResponse("catalog-service", "Missing or invalid Authorization header"))
        return null
    }
    val token = hdr.removePrefix("Bearer ").trim()
    val user = validateTokenViaAuthService(token)
    if (user == null) {
        call.respond(HttpStatusCode.Unauthorized, ApiResponse("catalog-service", "Invalid or expired token"))
        return null
    }
    if (requiredRole != null && !user.roles.contains(requiredRole)) {
        call.respond(HttpStatusCode.Forbidden, ApiResponse("catalog-service", "Insufficient permissions"))
        return null
    }
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

fun main() {
    val logger = LoggerFactory.getLogger("CatalogService")
    val rabbitmq = RabbitMQPublisher()

    rabbitmq.connect()
    Runtime.getRuntime().addShutdownHook(Thread { rabbitmq.close() })

    embeddedServer(Netty, port = 8080, host = "0.0.0.0") {
        install(ContentNegotiation) { json() }

        routing {
            // Треки
            route("/api/catalog/tracks") {

                get {
                    val user = authenticateOrReject(call, requiredRole = "USER") ?: return@get
                    val tracks = listOf(
                        Track(1, "Bohemian Rhapsody", "Queen", "A Night at the Opera", 354, "Rock"),
                        Track(2, "Imagine", "John Lennon", "Imagine", 183, "Pop"),
                        Track(3, "Smells Like Teen Spirit", "Nirvana", "Nevermind", 301, "Grunge")
                    )

                    // Без mapOf<String, Any>: используем buildJsonObject
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

                    call.respond(createResponse("catalog-service", "Список всех треков", tracks))
                }

                get("/{id}") {
                    val user = authenticateOrReject(call, requiredRole = "USER") ?: return@get
                    val id = call.parameters["id"]?.toIntOrNull()
                    if (id == null) {
                        call.respond(HttpStatusCode.BadRequest, ApiResponse("catalog-service", "Некорректный ID трека"))
                        return@get
                    }

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
