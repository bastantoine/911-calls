//const elasticsearch = require('elasticsearch');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');

const ELASTIC_SEARCH_URI = 'http://localhost:9200';
const INDEX_NAME = '911-calls';

async function run() {
  const client = new Client({ node: ELASTIC_SEARCH_URI});

  // Drop index if exists
  console.log(`Dropping index ${INDEX_NAME}`);
  await client.indices.delete({
    index: INDEX_NAME,
    ignore_unavailable: true
  });
  console.log(`└ Dropped index ${INDEX_NAME}`);

  console.log(`Creating index ${INDEX_NAME}`);
  await client.indices.create({
    index: INDEX_NAME,
    body : {
      mappings: {
        properties: {
          point: { type: 'geo_point' },
          desc: { type: 'text' },
          zip: { type: 'integer' },
          category: {
            type: 'text',
            // Setup multi-type field to be able to aggregate on the category.
            // From https://www.elastic.co/guide/en/elasticsearch/reference/current/multi-fields.html
            fields: {
              raw: {
                type: "keyword"
              }
            }
          },
          title: {
            type: 'text',
            // Setup multi-type field to be able to aggregate on the category.
            // From https://www.elastic.co/guide/en/elasticsearch/reference/current/multi-fields.html
            fields: {
              raw: {
                type: "keyword"
              }
            }
          },
          timeStamp: {
            type: 'date',
            format: "yyyy-MM-dd HH:mm:ss"
          },
          twp: {
            type: 'text',
            // Setup multi-type field to be able to aggregate on the category.
            // From https://www.elastic.co/guide/en/elasticsearch/reference/current/multi-fields.html
            fields: {
              raw: {
                type: "keyword"
              }
            }
          },
          addr: { type: 'text' }
        }
      }
    }
  });
  console.log(`└ Created index ${INDEX_NAME}`);

  let all_calls = [];
  fs.createReadStream('../911.csv')
    .pipe(csv())
    .on('data', data => {
      all_calls.push({
        point: { lat: data.lat, lon: data.lng },
        desc: data.desc,
        zip: data.zip,
        category: data.title.split(':')[0],
        title: data.title,
        timeStamp: data.timeStamp,
        twp: data.twp,
        addr: data.addr
      });
    })
    .on('end', async () => {
      console.log(`${all_calls.length} calls to insert`);
      const body = all_calls.flatMap((call) => [{ index: { _index: INDEX_NAME, _type: '_doc' } }, call]);
      client.bulk({ body }, (err, resp) => {
        if (err) console.trace(err.message);
        else console.log(`└ ${resp.body.items.length} calls inserted`);
        client.close();
      })
    });
}

run().catch(console.log);
