# flink-artisans-training
Examples for http://dataartisans.github.io/flink-training

## Project setup
Following http://dataartisans.github.io/flink-training/devSetup/handsOn.html, and editing the resulting project

```bash
mvn archetype:generate -DarchetypeGroupId=org.apache.flink -DarchetypeArtifactId=flink-quickstart-scala  -DarchetypeVersion=1.0.0 -DgroupId=org.apache.flink.quickstart -DartifactId=flink-scala-project -Dversion=0.1 -Dpackage=org.apache.flink.quickstart -DinteractiveMode=false
```
then clone and install the `flink-training-exercises` project

```bash
git clone https://github.com/dataArtisans/flink-training-exercises.git
cd flink-training-exercises
mvn clean install
```

and add the dependency to the pom for this project. Note this project will only compile of machines where the `flink-training-exercises` project is installed. The project compiles ok as follows: 

```
juanrh@juanyDell MINGW64 ~/git/flink-artisans-training (master)
$ mvn clean package
```


### Scala version issue with Scala IDE 
This works, but when opening the project in Scala IDE, after changing the scala compiler setting to use Scala 2.10 (use ` -Xsource:2.10 -Ymacro-expand:none`), we get a warning in the IDE `can't expand macros compiled by previous versions of Scala`. The program executes ok in the IDE, but the type information provided is limited. 

To fix this we can just update all the dependencies to the Scala 2.11 artifacts, and recompiling with `mvn clean install` it works ok, so it looks the artifact for `flink-training-exercises` is independent of the scala version. Then we can restore the value of the Scala IDE option for the Scala version to use the latest 2.11, and it works ok, but the warning in gone and we have the full type information available in the IDE