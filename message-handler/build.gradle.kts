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
    // RabbitMQ
    implementation("com.rabbitmq:amqp-client:5.20.0")

    // JSON
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.2")

    // Logging
    implementation("ch.qos.logback:logback-classic:1.4.14")
    implementation("org.slf4j:slf4j-api:2.0.9")
    // Logstash encoder для JSON логирования
    implementation("net.logstash.logback:logstash-logback-encoder:7.4")
}

application {
    mainClass.set("MainKt")
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "MainKt"
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
}