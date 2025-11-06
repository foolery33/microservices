plugins {
    kotlin("jvm") version "1.9.22"
    kotlin("plugin.serialization") version "1.9.22"
    application
}

group = "com.music"
version = "1.0.0"

repositories {
    mavenCentral()
}

dependencies {
    // Ktor
    implementation("io.ktor:ktor-server-core:2.3.7")
    implementation("io.ktor:ktor-server-netty:2.3.7")
    implementation("io.ktor:ktor-server-content-negotiation:2.3.7")
    implementation("io.ktor:ktor-serialization-kotlinx-json:2.3.7")

    // RabbitMQ
    implementation("com.rabbitmq:amqp-client:5.20.0")

    // Logging
    implementation("ch.qos.logback:logback-classic:1.4.14")
    // Logstash encoder для JSON логирования
    implementation("net.logstash.logback:logstash-logback-encoder:7.4")
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "MainKt"
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
}