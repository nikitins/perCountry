cd pergeo/
ssh gu_gateway 'rm pergeo_2.11-0.2.jar'
sbt package
scp target/scala-2.11/pergeo_2.11-0.2.jar gu_gateway:/home/snikitin/

