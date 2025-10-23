import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.plugins.contentnegotiation.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.encodeToString
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.Connection
import com.rabbitmq.client.Channel
import org.slf4j.LoggerFactory

@Serializable
data class Playlist(
    val id: Int,
    val name: String,
    val userId: Int,
    val tracksCount: Int,
    val isPublic: Boolean,
    val description: String
)

@Serializable
data class PlaylistTrack(
    val playlistId: Int,
    val trackId: Int,
    val trackTitle: String,
    val artist: String,
    val addedAt: String
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

// RabbitMQ Helper
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

            // Создаем exchange
            channel?.exchangeDeclare(EXCHANGE_NAME, "topic", true)

            // Создаем очередь
            channel?.queueDeclare(PLAYLIST_QUEUE, true, false, false, null)

            // Привязываем очередь к exchange
            channel?.queueBind(PLAYLIST_QUEUE, EXCHANGE_NAME, "playlist.#")

            logger.info("✅ Connected to RabbitMQ: ${factory.host}:${factory.port}")
        } catch (e: Exception) {
            logger.error("❌ Failed to connect to RabbitMQ: ${e.message}")
        }
    }

    fun publishEvent(routingKey: String, event: EventMessage) {
        try {
            val message = Json.encodeToString(event)
            channel?.basicPublish(EXCHANGE_NAME, routingKey, null, message.toByteArray())
            logger.info("📤 Published event: $routingKey -> ${event.eventType}")
        } catch (e: Exception) {
            logger.error("❌ Failed to publish event: ${e.message}")
        }
    }

    fun close() {
        channel?.close()
        connection?.close()
        logger.info("🔌 Disconnected from RabbitMQ")
    }
}

inline fun <reified T> createResponse(service: String, message: String, data: T): ApiResponse {
    return ApiResponse(
        service = service,
        message = message,
        data = Json.encodeToJsonElement(data)
    )
}

fun main() {
    val logger = LoggerFactory.getLogger("PlaylistService")
    val rabbitmq = RabbitMQPublisher()

    // Подключаемся к RabbitMQ
    rabbitmq.connect()

    // Shutdown hook
    Runtime.getRuntime().addShutdownHook(Thread {
        rabbitmq.close()
    })

    embeddedServer(Netty, port = 8080, host = "0.0.0.0") {
        install(ContentNegotiation) {
            json()
        }

        routing {
            route("/api/playlists") {
                get {
                    val playlists = listOf(
                        Playlist(1, "My Favorites", 1, 25, true, "Моя любимая музыка"),
                        Playlist(2, "Workout Mix", 1, 40, false, "Музыка для тренировок"),
                        Playlist(3, "Chill Vibes", 2, 15, true, "Расслабляющая музыка")
                    )

                    // Отправляем событие в RabbitMQ
                    rabbitmq.publishEvent(
                        "playlist.list",
                        EventMessage(
                            eventType = "PLAYLISTS_LISTED",
                            service = "playlist-service",
                            timestamp = System.currentTimeMillis(),
                            data = Json.encodeToJsonElement(mapOf("count" to playlists.size))
                        )
                    )

                    call.respond(createResponse("playlist-service", "Список всех плейлистов", playlists))
                }

                get("/{id}") {
                    val id = call.parameters["id"]?.toIntOrNull()
                    if (id == null) {
                        call.respond(
                            HttpStatusCode.BadRequest,
                            ApiResponse("playlist-service", "Некорректный ID плейлиста", null)
                        )
                        return@get
                    }

                    val playlist = Playlist(id, "Playlist #$id", 1, 10, true, "Description")

                    // Отправляем событие
                    rabbitmq.publishEvent(
                        "playlist.view",
                        EventMessage(
                            eventType = "PLAYLIST_VIEWED",
                            service = "playlist-service",
                            timestamp = System.currentTimeMillis(),
                            data = Json.encodeToJsonElement(playlist)
                        )
                    )

                    call.respond(createResponse("playlist-service", "Информация о плейлисте", playlist))
                }

                get("/{id}/tracks") {
                    val id = call.parameters["id"]?.toIntOrNull()
                    if (id == null) {
                        call.respond(
                            HttpStatusCode.BadRequest,
                            ApiResponse("playlist-service", "Некорректный ID плейлиста", null)
                        )
                        return@get
                    }

                    val tracks = listOf(
                        PlaylistTrack(id, 1, "Bohemian Rhapsody", "Queen", "2024-01-15T10:30:00"),
                        PlaylistTrack(id, 5, "Imagine", "John Lennon", "2024-01-16T14:20:00")
                    )

                    // Отправляем событие
                    rabbitmq.publishEvent(
                        "playlist.tracks.view",
                        EventMessage(
                            eventType = "PLAYLIST_TRACKS_VIEWED",
                            service = "playlist-service",
                            timestamp = System.currentTimeMillis(),
                            data = Json.encodeToJsonElement(mapOf(
                                "playlistId" to id,
                                "tracksCount" to tracks.size
                            ))
                        )
                    )

                    call.respond(createResponse("playlist-service", "Треки плейлиста #$id", tracks))
                }

                get("/user/{userId}") {
                    val userId = call.parameters["userId"]?.toIntOrNull()
                    if (userId == null) {
                        call.respond(
                            HttpStatusCode.BadRequest,
                            ApiResponse("playlist-service", "Некорректный ID пользователя", null)
                        )
                        return@get
                    }

                    val playlists = listOf(
                        Playlist(100, "User $userId Playlist", userId, 8, true, "Мой плейлист")
                    )

                    // Отправляем событие
                    rabbitmq.publishEvent(
                        "playlist.user.view",
                        EventMessage(
                            eventType = "USER_PLAYLISTS_VIEWED",
                            service = "playlist-service",
                            timestamp = System.currentTimeMillis(),
                            data = Json.encodeToJsonElement(mapOf(
                                "userId" to userId,
                                "playlistsCount" to playlists.size
                            ))
                        )
                    )

                    call.respond(createResponse("playlist-service", "Плейлисты пользователя #$userId", playlists))
                }
            }
        }
    }.start(wait = true)
}