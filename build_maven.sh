MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=256m" mvn -Dhadoop.version=2.2.0 -Phive -Phive-0.13.1 -Phive-thriftserver -DskipTests -Dscalastyle.failOnViolation=false clean
#MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=256m" mvn -Dhadoop.version=2.2.0 -Phive -Phive-0.13.1 -Phive-thriftserver -DskipTests -Dscalastyle.failOnViolation=false install package
#sbt/sbt -Dhadoop.version=2.2.0 -Phive -Phive-0.13.1 -Phive-thriftserver gen-idea
#./make-distribution.sh --tgz -Dhadoop.version=2.2.0 -Phive -Phive-0.13.1 -Phive-thriftserver
