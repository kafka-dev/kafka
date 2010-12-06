#!/bin/sh
#
VERSION="0.05"

(cd ..;ant clean jar docs)
(cd ../src; zip -r ../maven/kafka-$VERSION-sources.jar kafka)
(cd ../docs; zip -r ../maven/kafka-$VERSION-apidocs.jar *)

mvn install:install-file -DpomFile=pom.xml \
                         -Dfile=../dist/kafka-$VERSION.jar \
                         -Dpackaging=jar

mvn install:install-file -DpomFile=pom.xml \
                         -Dfile=kafka-$VERSION-sources.jar \
                         -Dclassifier=sources \
                         -Dpackaging=jar

mvn install:install-file -DpomFile=pom.xml \
                         -Dfile=kafka-$VERSION-apidocs.jar \
                         -Dclassifier=javadoc \
                         -Dpackaging=jar

rm -f kafka*.jar
