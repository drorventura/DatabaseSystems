main:
    javac -cp $(wildcard *.jar) Main.java LoadFiles.java PrintTable.java Query.java ReadCVS.java Schema.java
    java -cp .:./$(wildcard *.jar) Main

clean:
    rm *.class