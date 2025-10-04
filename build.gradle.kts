import net.researchgate.release.ReleaseExtension

plugins {
    id("io.github.gradle-nexus.publish-plugin") version "2.0.0"
    id("net.researchgate.release") version "3.1.0"
}

configure<ReleaseExtension> {
    with(git) {
        requireBranch.set("master")
    }
}

nexusPublishing {
    repositories {
        sonatype {
            nexusUrl.set(uri("https://ossrh-staging-api.central.sonatype.com/service/local/"))
            snapshotRepositoryUrl.set(uri("https://central.sonatype.com/repository/maven-snapshots/"))

            // defaults to project.properties["myNexusUsername"]
            username.set(findProperty("ossrhUsername") as? String ?: "unset")
            // defaults to project.properties["myNexusPassword"]
            password.set(findProperty("ossrhPassword") as? String ?: "unset")
        }
    }
}

val flinkVersion by extra("1.20.0")
