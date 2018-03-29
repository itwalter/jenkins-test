pipeline {
    agent any
    environment {
    //Use Pipeline Utility Steps plugin to read information from pom.xml into env variables
    IMAGE = readMavenPom().getArtifactId()
    VERSION = readMavenPom().getVersion()
    }    
    stages {
        stage('Example') {
            steps {
                echo 'Hello World'
                sh 'pwd'
                sh 'echo $VERSION'
                sh 'whoami'
                echo $VERSION
            }
        }
    }
    post { 
        always { 
            echo 'I will always say Hello again!'
        }
    }
}
