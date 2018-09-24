let sbt = startSbt({
    sh: "sbt -DdevMode=true",
    watch: ["build.sbt"]
});

let compile = sbt.run({
    command: "compile",
    watch: ["**/*.scala"]
});

let generateClasspath = sbt.run({
    name: "classpaths",
    command: "writeClasspath"
});

let database = runServer({
    name: "mysqld",
    httpPort: 3388,
    sh: "java -cp `cat /tmp/classpath_com.criteo.cuttle.localdb` com.criteo.cuttle.localdb.LocalDB"
}).dependsOn(generateClasspath);

let yarn = run({
    sh: "yarn install",
    watch: "package.json"
});

let front = webpack({
    watch: [
        "webpack.config.js",
        "timeseries/src/main/javascript/**/*.*",
        "timeseries/src/main/html/**/*.*",
        "timeseries/src/main/style/**/*.*"
    ]
}).dependsOn(yarn, compile);

let server = runServer({
    httpPort: 8888,
    sh: "java -cp `cat /tmp/classpath_com.criteo.cuttle.examples` com.criteo.cuttle.examples.HelloTimeSeries",
    env: {
        CUTTLE_PG_URI: "psql2.gp.naumen.ru:5432",
        CUTTLE_PG_DATABASE: "ybushuev_cuttle",
        CUTTLE_PG_USERNAME: "cuttle",
        CUTTLE_PG_PASSWORD: "cuttle"
    }
}).dependsOn(compile, generateClasspath);

proxy(server, 8080).dependsOn(front);
