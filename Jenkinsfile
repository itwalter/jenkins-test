pipeline {
    agent any
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
