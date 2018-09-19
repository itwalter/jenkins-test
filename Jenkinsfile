pipeline {
    agent any
    stages {
        stage('Example') {
            steps {
                echo 'Hello World'
                sh 'pwd'
                sh 'echo $VERSION'
                sh 'whoami'
                sh 'ls'
                sh 'ansible --version'
                sh 'cat build.sh'
            }
        }
    }
    post { 
        always { 
            echo 'I will always say Hello again!'
        }
    }
}
