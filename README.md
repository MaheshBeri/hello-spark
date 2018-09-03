sudo update-alternatives --config javac
sudo update-alternatives --config java
openjdk version "1.8.0_171"
pom xml //add  compiler plugin <source>8</source>
mvn package
spark-submit --class "SimpleApp"   --master local[4]   target/simple-project-1.0.jar
