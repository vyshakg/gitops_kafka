const yaml = require('js-yaml');
const fs = require('fs');
const { default: axios } = require('axios');

const GITOPS_KAFKA_ROOT = "../../plm_gitops-kafka";
const KAFKA_REST_HOST = "http://localhost:8082";
const KAFKA_SCHMEA_HOST = "http://localhost:8081";
const SCHMEA_USERNAME = "kafkauser";
const SCHMEA_PASSWORD = "admin@2022"


//Read the Yaml file
const data = fs.readFileSync(`${GITOPS_KAFKA_ROOT}/clusters/plm_non_prod_cl_01/plm-test/topics.yaml`, 'utf8');

//Convert Yml object to JSON
const yamlData = yaml.load(data);


const main = async () => {
    const clustersResponse = await axios.get(`${KAFKA_REST_HOST}/v3/clusters`, { headers: { 'Content-Type': 'application/vnd.api+json' } });
    const cluster_id = clustersResponse.data.data[0].cluster_id

    let total_topics = 0;
    let created_topics = 0;
    let skipped_topics = 0;
    let schema_count = 0;


    let envs = ["test", "dev", "prod"]

    for (const topic of yamlData.topics) {
        total_topics++;

        for (const env of envs) {

            let topicName = topic.name;
            if (env != "test")
                topicName = topicName.replace(/^test/g, env);


            // get clusterId
            var options = {
                method: 'POST',
                url: `${KAFKA_REST_HOST}/v3/clusters/${cluster_id}/topics`,
                headers: { 'Content-Type': 'application/json' },
                data: {
                    topic_name: topicName,
                    partitions_count: 1,
                    configs: [{ name: 'cleanup.policy', value: 'delete' }]
                }
            };


            // create topics
            try {
                await axios.request(options);
                created_topics++;
            } catch (error) {
                if (error.response) {
                    skipped_topics++
                    if (error.response.data.error_code != 40002) // 40002 -  topics already exists
                        console.error(error.response.data.message)
                }
            }

            
            if (topic.schema && topic.schema.valueFilePath) {

                schema_count++;
                var options = {
                    method: 'DELETE',
                    url: `${KAFKA_SCHMEA_HOST}/subjects/${topicName}-value`,
                    params: { permanent: 'false' },
                    auth: {
                        username: SCHMEA_USERNAME,
                        password: SCHMEA_PASSWORD
                    }
                };

                try {
                    await axios.request(options);
                } catch (error) {
                    if (error.response) {
                        if (error.response.data.error_code != 40401) // 40401 -  Subject not found.
                            console.error(error.response.data.message)
                    }
                }

                try {
                    await axios.request({ ...options, params: { permanent: 'true' } });
                } catch (error) {
                    if (error.response) {
                        if (error.response.data.error_code != 40401) // 40401 -  Subject not found.
                            console.error(error.response.data.message)
                    }
                }



                let schema = fs.readFileSync(`${GITOPS_KAFKA_ROOT}/${topic.schema.valueFilePath}`, 'utf8');
                const schemaString = JSON.stringify(schema);
                let schemaType = schemaString.includes("json-schema") ? "JSON" : "AVRO";

                var options = {
                    method: 'POST',
                    url: `${KAFKA_SCHMEA_HOST}/subjects/${topicName}-value/versions`,
                    headers: {
                        'Content-Type': 'application/vnd.schemaregistry.v1+json',
                    },
                    auth: {
                        username: SCHMEA_USERNAME,
                        password: SCHMEA_PASSWORD
                    },
                    data: { schema: schema, schemaType }
                };

                try {
                    await axios.request(options);
                } catch (error) {
                    if (error.response) {
                        console.error(error.response.data)
                    }
                }
            }
        }

    };

    console.log(`Total topics: ${total_topics * 3}`);
    console.log(`Created topics: ${created_topics} and skipped topics: ${skipped_topics} (Already created)`);
    console.log(`No. schema updated/created (Each time it will be hard deleted and created again): ${schema_count} \n`)

}

main().then(() => console.log("===== Sync completed ===== \n "))
    .catch(err => console.error(err));