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
            route("/api/playlists") {
                get {
                    val playlists = listOf(
                        Playlist(1, "My Favorites", 1, 25, true, "Моя любимая музыка"),
                        Playlist(2, "Workout Mix", 1, 40, false, "Музыка для тренировок"),
                        Playlist(3, "Chill Vibes", 2, 15, true, "Расслабляющая музыка")
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
                    call.respond(createResponse("playlist-service", "Плейлисты пользователя #$userId", playlists))
                }
            }
        }
    }.start(wait = true)
}