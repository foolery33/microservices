import com.rabbitmq.client.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import org.slf4j.LoggerFactory
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.*

@Serializable
data class EventMessage(
    val eventType: String,
    val service: String,
    val timestamp: Long,
    val data: JsonElement
)

class MessageHandler {
    private val logger = LoggerFactory.getLogger(MessageHandler::class.java)
    private var connection: Connection? = null
    private var channel: Channel? = null
    private val dateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    companion object {
        const val EXCHANGE_NAME = "music_events"
        const val CATALOG_QUEUE = "catalog_events"
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

            logger.info("=".repeat(60))
            logger.info("🚀 Message Handler Service Started")
            logger.info("📡 Connected to RabbitMQ: ${factory.host}:${factory.port}")
            logger.info("=".repeat(60))

        } catch (e: Exception) {
            logger.error("❌ Failed to connect to RabbitMQ: ${e.message}")
            throw e
        }
    }

    fun startConsuming() {
        channel?.let { ch ->
            // Обработчик для catalog_events
            val catalogConsumer = object : DefaultConsumer(ch) {
                override fun handleDelivery(
                    consumerTag: String?,
                    envelope: Envelope?,
                    properties: AMQP.BasicProperties?,
                    body: ByteArray?
                ) {
                    try {
                        val message = String(body ?: byteArrayOf(), StandardCharsets.UTF_8)
                        val event = Json.decodeFromString<EventMessage>(message)

                        val timestamp = dateFormat.format(Date(event.timestamp))

                        logger.info("")
                        logger.info("=".repeat(80))
                        logger.info("📨 NEW MESSAGE RECEIVED")
                        logger.info("-".repeat(80))
                        logger.info("🔖 Routing Key: ${envelope?.routingKey}")
                        logger.info("📦 Queue: $CATALOG_QUEUE")
                        logger.info("🏷️  Event Type: ${event.eventType}")
                        logger.info("⚙️  Service: ${event.service}")
                        logger.info("🕐 Timestamp: $timestamp")
                        logger.info("📄 Data: ${event.data}")
                        logger.info("=".repeat(80))
                        logger.info("")

                        // Подтверждаем получение сообщения
                        ch.basicAck(envelope?.deliveryTag ?: 0, false)

                    } catch (e: Exception) {
                        logger.error("❌ Error processing message: ${e.message}")
                    }
                }
            }

            // Обработчик для playlist_events
            val playlistConsumer = object : DefaultConsumer(ch) {
                override fun handleDelivery(
                    consumerTag: String?,
                    envelope: Envelope?,
                    properties: AMQP.BasicProperties?,
                    body: ByteArray?
                ) {
                    try {
                        val message = String(body ?: byteArrayOf(), StandardCharsets.UTF_8)
                        val event = Json.decodeFromString<EventMessage>(message)

                        val timestamp = dateFormat.format(Date(event.timestamp))

                        logger.info("")
                        logger.info("=".repeat(80))
                        logger.info("📨 NEW MESSAGE RECEIVED")
                        logger.info("-".repeat(80))
                        logger.info("🔖 Routing Key: ${envelope?.routingKey}")
                        logger.info("📦 Queue: $PLAYLIST_QUEUE")
                        logger.info("🏷️  Event Type: ${event.eventType}")
                        logger.info("⚙️  Service: ${event.service}")
                        logger.info("🕐 Timestamp: $timestamp")
                        logger.info("📄 Data: ${event.data}")
                        logger.info("=".repeat(80))
                        logger.info("")

                        ch.basicAck(envelope?.deliveryTag ?: 0, false)

                    } catch (e: Exception) {
                        logger.error("❌ Error processing message: ${e.message}")
                    }
                }
            }

            // Подписываемся на очереди
            ch.basicConsume(CATALOG_QUEUE, false, catalogConsumer)
            ch.basicConsume(PLAYLIST_QUEUE, false, playlistConsumer)

            logger.info("👂 Listening for messages on queues:")
            logger.info("   • $CATALOG_QUEUE")
            logger.info("   • $PLAYLIST_QUEUE")
            logger.info("")
        }
    }

    fun close() {
        channel?.close()
        connection?.close()
        logger.info("🔌 Disconnected from RabbitMQ")
    }
}

fun main() {
    val handler = MessageHandler()

    // Shutdown hook
    Runtime.getRuntime().addShutdownHook(Thread {
        handler.close()
    })

    // Подключаемся и начинаем слушать
    handler.connect()
    handler.startConsuming()

    // Держим приложение запущенным
    while (true) {
        Thread.sleep(1000)
    }
}