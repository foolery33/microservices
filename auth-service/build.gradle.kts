plugins {
    kotlin("jvm") version "1.9.22"
    kotlin("plugin.serialization") version "1.9.22"
    application
}

application {
    mainClass.set("MainKt")
}

group = "com.music"
version = "1.0.0"

repositories { mavenCentral() }

dependencies {
    implementation("io.ktor:ktor-server-core:2.3.7")
    implementation("io.ktor:ktor-server-netty:2.3.7")
    implementation("io.ktor:ktor-server-content-negotiation:2.3.7")
    implementation("io.ktor:ktor-serialization-kotlinx-json:2.3.7")
    implementation("io.ktor:ktor-server-auth:2.3.7")
    implementation("ch.qos.logback:logback-classic:1.4.14")
    // Для генерации и валидации JWT
    implementation("com.auth0:java-jwt:4.4.0")
}

// Упаковка в fat‑jar
tasks.jar {
    manifest { attributes["Main-Class"] = "MainKt" }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
}
