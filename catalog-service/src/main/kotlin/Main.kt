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
    val data: JsonElement? = null  // Заменили Any? на JsonElement?
)

// Вспомогательная функция для удобного создания ответов
inline fun <reified T> createResponse(service: String, message: String, data: T): ApiResponse {
    return ApiResponse(
        service = service,
        message = message,
        data = Json.encodeToJsonElement(data)
    )
}

fun main() {
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
                    call.respond(createResponse("catalog-service", "Информация о треке", track))
                }

                get("/search") {
                    val query = call.request.queryParameters["q"] ?: ""
                    val result = listOf(Track(99, query, "Artist", "Album", 200, "Pop"))
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
                    call.respond(createResponse("catalog-service", "Информация об артисте", artist))
                }
            }
        }
    }.start(wait = true)
}