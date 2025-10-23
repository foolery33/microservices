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

            // Создаем exchange типа "topic"
            channel?.exchangeDeclare(EXCHANGE_NAME, "topic", true)

            // Создаем очередь
            channel?.queueDeclare(CATALOG_QUEUE, true, false, false, null)

            // Привязываем очередь к exchange
            channel?.queueBind(CATALOG_QUEUE, EXCHANGE_NAME, "catalog.#")

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
    val logger = LoggerFactory.getLogger("CatalogService")
    val rabbitmq = RabbitMQPublisher()

    // Подключаемся к RabbitMQ при старте
    rabbitmq.connect()

    // Добавляем shutdown hook для корректного закрытия соединения
    Runtime.getRuntime().addShutdownHook(Thread {
        rabbitmq.close()
    })

    embeddedServer(Netty, port = 8080, host = "0.0.0.0") {
        install(ContentNegotiation) {
            json()
        }

        routing {
            // Треки
            route("/api/catalog/tracks") {
                get {
                    val tracks = listOf(
                        Track(1, "Bohemian Rhapsody", "Queen", "A Night at the Opera", 354, "Rock"),
                        Track(2, "Imagine", "John Lennon", "Imagine", 183, "Pop"),
                        Track(3, "Smells Like Teen Spirit", "Nirvana", "Nevermind", 301, "Grunge")
                    )

                    // Отправляем событие в RabbitMQ
                    rabbitmq.publishEvent(
                        "catalog.tracks.list",
                        EventMessage(
                            eventType = "TRACKS_LISTED",
                            service = "catalog-service",
                            timestamp = System.currentTimeMillis(),
                            data = Json.encodeToJsonElement(mapOf("count" to tracks.size))
                        )
                    )

                    call.respond(createResponse("catalog-service", "Список всех треков", tracks))
                }

                get("/{id}") {
                    val id = call.parameters["id"]?.toIntOrNull()
                    if (id == null) {
                        call.respond(
                            HttpStatusCode.BadRequest,
                            ApiResponse("catalog-service", "Некорректный ID трека", null)
                        )
                        return@get
                    }

                    val track = Track(id, "Track #$id", "Artist", "Album", 180, "Rock")

                    // Отправляем событие
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
                    val query = call.request.queryParameters["q"] ?: ""
                    val result = listOf(Track(99, query, "Artist", "Album", 200, "Pop"))

                    // Отправляем событие поиска
                    rabbitmq.publishEvent(
                        "catalog.tracks.search",
                        EventMessage(
                            eventType = "TRACK_SEARCHED",
                            service = "catalog-service",
                            timestamp = System.currentTimeMillis(),
                            data = Json.encodeToJsonElement(mapOf(
                                "query" to query,
                                "results" to result.size
                            ))
                        )
                    )

                    call.respond(createResponse("catalog-service", "Поиск треков: $query", result))
                }
            }

            // Альбомы
            route("/api/catalog/albums") {
                get {
                    val albums = listOf(
                        Album(1, "Abbey Road", "The Beatles", 1969, 17),
                        Album(2, "Dark Side of the Moon", "Pink Floyd", 1973, 10),
                        Album(3, "Thriller", "Michael Jackson", 1982, 9)
                    )

                    rabbitmq.publishEvent(
                        "catalog.albums.list",
                        EventMessage(
                            eventType = "ALBUMS_LISTED",
                            service = "catalog-service",
                            timestamp = System.currentTimeMillis(),
                            data = Json.encodeToJsonElement(mapOf("count" to albums.size))
                        )
                    )

                    call.respond(createResponse("catalog-service", "Список альбомов", albums))
                }

                get("/{id}") {
                    val id = call.parameters["id"]?.toIntOrNull()
                    if (id == null) {
                        call.respond(
                            HttpStatusCode.BadRequest,
                            ApiResponse("catalog-service", "Некорректный ID альбома", null)
                        )
                        return@get
                    }

                    val album = Album(id, "Album #$id", "Artist", 2024, 12)

                    rabbitmq.publishEvent(
                        "catalog.albums.view",
                        EventMessage(
                            eventType = "ALBUM_VIEWED",
                            service = "catalog-service",
                            timestamp = System.currentTimeMillis(),
                            data = Json.encodeToJsonElement(album)
                        )
                    )

                    call.respond(createResponse("catalog-service", "Информация об альбоме", album))
                }
            }

            // Артисты
            route("/api/catalog/artists") {
                get {
                    val artists = listOf(
                        Artist(1, "The Beatles", "Rock", 13),
                        Artist(2, "Miles Davis", "Jazz", 50),
                        Artist(3, "Daft Punk", "Electronic", 4)
                    )

                    rabbitmq.publishEvent(
                        "catalog.artists.list",
                        EventMessage(
                            eventType = "ARTISTS_LISTED",
                            service = "catalog-service",
                            timestamp = System.currentTimeMillis(),
                            data = Json.encodeToJsonElement(mapOf("count" to artists.size))
                        )
                    )

                    call.respond(createResponse("catalog-service", "Список артистов", artists))
                }

                get("/{id}") {
                    val id = call.parameters["id"]?.toIntOrNull()
                    if (id == null) {
                        call.respond(
                            HttpStatusCode.BadRequest,
                            ApiResponse("catalog-service", "Некорректный ID артиста", null)
                        )
                        return@get
                    }

                    val artist = Artist(id, "Artist #$id", "Rock", 5)

                    rabbitmq.publishEvent(
                        "catalog.artists.view",
                        EventMessage(
                            eventType = "ARTIST_VIEWED",
                            service = "catalog-service",
                            timestamp = System.currentTimeMillis(),
                            data = Json.encodeToJsonElement(artist)
                        )
                    )

                    call.respond(createResponse("catalog-service", "Информация об артисте", artist))
                }
            }
        }
    }.start(wait = true)
}
