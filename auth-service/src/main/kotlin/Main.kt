import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import kotlinx.serialization.Serializable
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.util.Date
import java.util.UUID

// ===== JWT =====
private const val secret = "very-secret-key"
private val algorithm = Algorithm.HMAC256(secret)
private const val issuer = "auth-service"
private const val tokenValidityMs = 60 * 60 * 1000L  // 1 час

// ===== Logger =====
private val logger = LoggerFactory.getLogger("AuthService")

// ===== In-memory users =====
val users = mutableMapOf<String, UserRecord>() // key: username

@Serializable
data class Credentials(val username: String, val password: String)

@Serializable
data class AuthUser(
    val id: String,
    val username: String,
    val roles: List<String> = emptyList()
)

data class UserRecord(
    val id: String,
    val username: String,
    val password: String,
    val roles: List<String> = listOf("USER")
)

@Serializable
data class TokenResponse(val token: String)

fun generateToken(rec: UserRecord): String =
    JWT.create()
        .withIssuer(issuer)
        .withSubject(rec.username)
        .withClaim("uid", rec.id)
        .withArrayClaim("roles", rec.roles.toTypedArray())
        .withExpiresAt(Date(System.currentTimeMillis() + tokenValidityMs))
        .sign(algorithm)

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
    embeddedServer(Netty, port = 8080, host = "0.0.0.0") {
        install(ContentNegotiation) { json() }
        configureRequestIdInterceptor()

        routing {
            // Health
            get("/api/auth/health") {
                logger.info("Health check requested")
                call.respond(mapOf("status" to "ok"))
            }

            // Регистрация
            post("/api/auth/register") {
                val creds = call.receive<Credentials>()
                logger.info("Registration attempt for username=${creds.username}")

                if (users.containsKey(creds.username)) {
                    logger.warn("Registration failed: user already exists username=${creds.username}")
                    call.respond(HttpStatusCode.BadRequest, mapOf("error" to "User exists"))
                    return@post
                }
                val rec = UserRecord(
                    id = UUID.randomUUID().toString(),
                    username = creds.username,
                    password = creds.password,
                    roles = listOf("USER")
                )
                users[creds.username] = rec

                logger.info("User registered successfully username=${creds.username}, userId=${rec.id}")
                call.respond(HttpStatusCode.Created, mapOf("message" to "Registered"))
            }

            // Логин -> выдача JWT с uid/roles
            post("/api/auth/login") {
                val creds = call.receive<Credentials>()
                logger.info("Login attempt for username=${creds.username}")

                val rec = users[creds.username]
                if (rec == null || rec.password != creds.password) {
                    logger.warn("Login failed: invalid credentials for username=${creds.username}")
                    call.respond(HttpStatusCode.Unauthorized, mapOf("error" to "Invalid credentials"))
                    return@post
                }
                val token = generateToken(rec)

                MDC.put("user", rec.username)
                logger.info("Login successful for username=${creds.username}, userId=${rec.id}")

                call.respond(TokenResponse(token))
            }

            // Валидация токена -> возврат AuthUser
            post("/api/auth/validate") {
                val authHeader = call.request.headers[HttpHeaders.Authorization]
                logger.info("Token validation requested")

                if (authHeader == null || !authHeader.startsWith("Bearer ")) {
                    logger.warn("Token validation failed: missing or invalid Authorization header")
                    call.respond(HttpStatusCode.Unauthorized, mapOf("error" to "Token required"))
                    return@post
                }
                val token = authHeader.substringAfter(' ').trim()
                try {
                    val verifier = JWT.require(algorithm).withIssuer(issuer).build()
                    val decoded = verifier.verify(token)

                    val uid = decoded.getClaim("uid").asString().orEmpty()
                    val username = decoded.subject.orEmpty()
                    val roles = decoded.getClaim("roles").asList(String::class.java) ?: emptyList()

                    if (username.isBlank()) {
                        logger.warn("Token validation failed: username is blank")
                        call.respond(HttpStatusCode.Unauthorized, mapOf("error" to "Invalid token"))
                        return@post
                    }

                    MDC.put("user", username)
                    logger.info("Token validated successfully for username=$username, userId=$uid")

                    call.respond(AuthUser(id = uid, username = username, roles = roles))
                } catch (ex: Exception) {
                    logger.error("Token validation error: ${ex.message}", ex)
                    call.respond(HttpStatusCode.Unauthorized, mapOf("error" to "Invalid token"))
                }
            }
        }
    }.start(wait = true)
}