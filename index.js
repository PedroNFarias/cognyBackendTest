const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const massive = require('massive');
const monitor = require('pg-monitor');
const apiData = require('./getApiData');
const readline = require('readline');

// Call start
(async () => {
    var data = await apiData.getData('Nation', 'Population');
    console.log('main.js: before start');

    const db = await massive({
        connectionString: DATABASE_URL,
        ssl: { rejectUnauthorized: false },
    }, {
        // Massive Configuration
        scripts: process.cwd() + '/migration',
        allowedSchemas: [DATABASE_SCHEMA],
        whitelist: [`${DATABASE_SCHEMA}.%`],
        excludeFunctions: true,
    }, {
        // Driver Configuration
        noWarnings: true,
        error: function (err, client) {
            console.log(err);
            //process.emit('uncaughtException', err);
            //throw err;
        }
    });

    if (!monitor.isAttached() && SHOW_PG_MONITOR === 'true') {
        monitor.attach(db.driverConfig);
    }

    const execFileSql = async (schema, type) => {
        return new Promise(async resolve => {
            const objects = db['user'][type];

            if (objects) {
                for (const [key, func] of Object.entries(objects)) {
                    console.log(`executing ${schema} ${type} ${key}...`);
                    await func({
                        schema: DATABASE_SCHEMA,
                    });
                }
            }

            resolve();
        });
    };

    //public
    const migrationUp = async () => {
        return new Promise(async resolve => {
            await execFileSql(DATABASE_SCHEMA, 'schema');

            //cria as estruturas necessarias no db (schema)
            await execFileSql(DATABASE_SCHEMA, 'table');
            await execFileSql(DATABASE_SCHEMA, 'view');

            console.log(`reload schemas ...`)
            await db.reload();

            resolve();
        });
    };

    try {
        await migrationUp();

        //truncate table before insert
        await db[DATABASE_SCHEMA].api_data.destroy({});
        console.log('Limpar a tabela');
        for (var i = 0; i < data.length; i++) {
            const result1 = await db[DATABASE_SCHEMA].api_data.insert({
                doc_record: data[i]
            })
            //console.log('Dados inseridos: ', result1);
        }

        const result = data
            .filter(item => item.Year == 2020 || item.Year == 2019 || item.Year == 2018)
            .map(item => item.Population)
            .reduce((acc, cur) => acc + cur, 0);
        console.log('Resultado do insert: ', result);

        //b. usando uma query no banco de dados
        const result2 = await db.query(
            `
            SELECT SUM((doc_record->>'Population')::numeric) AS total_population
            FROM ${DATABASE_SCHEMA}.api_data
            WHERE
                (doc_record->>'Year')::int IN (2020, 2019, 2018)
            `
        );
        console.log('Resultado do select: ', result2);

        //d. Cria uma view no banco de dados com a query digitada pelo usuário e executa a view
        const result4 = await db.query(
            `
            CREATE OR REPLACE VIEW ${DATABASE_SCHEMA}.vw_totalPopulation AS
            SELECT SUM((doc_record->>'Population')::numeric) AS total_population
            FROM ${DATABASE_SCHEMA}.api_data
            WHERE
                (doc_record->>'Year')::int IN (2020, 2019, 2018);

            SELECT * FROM ${DATABASE_SCHEMA}.vw_totalPopulation
            `
        );
        console.log('Resultado do select da view: ', result4);

        //c. pedindo ao usuário para digitar uma query no console e executando a query no banco de dados
        const query = await askQuestion('Digite uma query (Caso não queira digite n): ');
        if(query != 'n'){
            const result3 = await db.query(query);
            console.log('Resultado do select: ', result3);
        }
    } catch (e) {
        console.log(e.message);
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');

})();

//Função auxiliar para solicitar dados ao usuário
function askQuestion(query) {
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout,
    });

    return new Promise(resolve => rl.question(query, ans => {
        rl.close();
        resolve(ans);
    }))
}