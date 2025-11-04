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
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.Connection
import com.rabbitmq.client.Channel
import java.net.HttpURLConnection
import java.net.URL

@Serializable
data class Playlist(
    val id: Int,
    val name: String,
    val tracks: List<Int> = emptyList(),
    val owner: String = "" // <-- дефолт, чтобы POST не требовал поле
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
        ?: "http://auth-service:8080/api/auth/validate"
    val url = URL(urlStr)

    println("🔍 [AuthValidator] Проверка токена через $urlStr")

    val conn = (url.openConnection() as HttpURLConnection).apply {
        requestMethod = "POST"
        doOutput = true
        setFixedLengthStreamingMode(0)
        setRequestProperty("Authorization", "Bearer $token")
        connectTimeout = 8000
        readTimeout = 8000
        instanceFollowRedirects = false
    }

    return try {
        conn.outputStream.use { /* пустое тело */ }
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
        call.respond(HttpStatusCode.Unauthorized, ApiResponse("playlist-service", "Missing or invalid Authorization header"))
        return null
    }
    val token = hdr.removePrefix("Bearer ").trim()
    val user = validateTokenViaAuthService(token) ?: run {
        call.respond(HttpStatusCode.Unauthorized, ApiResponse("playlist-service", "Invalid or expired token"))
        return null
    }
    if (requiredRole != null && !user.roles.contains(requiredRole)) {
        call.respond(HttpStatusCode.Forbidden, ApiResponse("playlist-service", "Insufficient permissions"))
        return null
    }
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
        } catch (e: Exception) {
            logger.error("❌ Failed to publish event: ${e.message}")
        }
    }

    fun close() {
        channel?.close()
        connection?.close()
    }
}

fun main() {
    val logger = LoggerFactory.getLogger("PlaylistService")
    val rabbitmq = RabbitMQPublisher()
    rabbitmq.connect()
    Runtime.getRuntime().addShutdownHook(Thread { rabbitmq.close() })

    embeddedServer(Netty, port = 8080, host = "0.0.0.0") {
        install(ContentNegotiation) { json() }

        routing {
            route("/api/playlists") {

                get {
                    val user = authenticateOrReject(call, requiredRole = "USER") ?: return@get

                    val playlists = listOf(
                        Playlist(1, "My Rock", listOf(1, 3), owner = user.username),
                        Playlist(2, "Chill", listOf(2), owner = user.username)
                    )

                    // buildJsonObject вместо mapOf<String, Any>
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
                        call.respond(HttpStatusCode.BadRequest, ApiResponse("playlist-service", "Некорректный ID плейлиста"))
                        return@get
                    }

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

                    val req = call.receive<Playlist>()  // owner может не приходить (дефолт есть)
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
