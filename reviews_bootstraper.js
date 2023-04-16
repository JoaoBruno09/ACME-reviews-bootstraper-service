const amqp = require('amqplib');
const { Client } = require('pg');

const client = new Client({
  user: 'postgres',
  host: 'db',
  database: 'reviewsBT',
  password: 'mysecretpassword',
  port: 5432, // The default PostgreSQL port
});

async function main() {

  // Connect to the database
  client.connect((err) => {
    if (err) {
      console.error('Error connecting to PostgreSQL database:', err.stack);
    } else {
      console.log('Connected to PostgreSQL database');
    }
  });
  //Products table
  client.query(`CREATE TABLE IF NOT EXISTS Products ( id SERIAL PRIMARY KEY, sku VARCHAR(255) NOT NULL, designation TEXT NOT NULL, description TEXT NOT NULL)`, (err, res) => {
    if (err) {
      console.error('Error performing query:', err.stack);
    } else {
      console.log('Query result:', res.rows);
      client.query(`SELECT x.* FROM public.products x
      WHERE sku = 'k485b1l47h5b'`, (err, res) => {
        if (err) {
          console.error('Error performing query:', err.stack);
        }
        else {
          console.log('Query result:', res.rows);
          if (res.rowCount == 0) {
            client.query(`INSERT INTO Products (sku, designation, description) VALUES 
              ('k485b1l47h5b', 'Placa Gráfica Gigabyte GeForce RTX 2060 D6 6G', 'placa mais accessivel e decente'),
              ('c475e9l47f5b', 'Cooler CPU Nox H-224 ARGB', 'arrefecedor com luzinhas'),
              ('n385j1l17h8s', 'Cooler CPU Noctua NH-D15 Chromax Black', 'arrefecedor sem luzinhas'),
              ('z205bgi47hoc', 'Rato Óptico Logitech Pro X Superlight Wireless 25400DPI ', 'rato com nome de pessoa'),
              ('h274h5l4563b', 'Rato Óptico Razer DeathAdder Essential 2021 6400DPI', 'rato da marca das cobrinhas'),
              ('b523h5l487kl', 'Portátil Lenovo Legion 5 15.6" 15ACH6', 'portatil todo fancy'),
              ('x258f5l48475', 'Portátil MSI Raider GE76 12UH-007XPT', 'portatil com luzinhas'),
              ('t364gju78354', 'Portátil MSI Stealth GS66 12UGS-009PT', 'Protatil pra quem pode'),
              ('c6348gn840j1', 'Computador Desktop MSI MAG Infinite S3 11SH-208XIB', 'desktop para o proximo feromonas'),
              ('q73f947gn681', 'Teclado Mecânico Razer Blackwidow V3 PT Tenkeyless RGB Yellow Switch', 'teclado cheio de cores mecanico'),
              ('pa23fvh509a1', 'Cartão Microsoft Office 2021 Casa e Estudantes 1 PC/MAC', 'autentico roubo'),
              ('d63h57d738s1', 'Seat Arona', 'La maquina de fiesta')`,
              (err, res) => {
                if (err) {
                  console.error('Error performing query:', err.stack);
                } else {
                  console.log('Query result:', res.rows);
                }
              })
          }
        }
      })
      


    }
    
  })
  
  //Reviews table
  client.query(`CREATE TABLE IF NOT EXISTS Reviews (
    "idReview" SERIAL PRIMARY KEY,
    version BIGINT NOT NULL,
    "approvalStatus" VARCHAR(255) NOT NULL,
    "reviewText" VARCHAR(2048) NOT NULL,
    "publishingDate" DATE NOT NULL,
    "funFact" VARCHAR(255) NOT NULL,
    vote BIGINT NOT NULL, 
    "productSku" VARCHAR(255) NOT NULL,
    "userId" BIGINT NOT NULL,
    "rid" VARCHAR(255) NOT NULL UNIQUE
  );`, (err, res) => {
    if (err) {
      console.error('Error performing query:', err.stack);
    } else {
      console.log('Query result:', res.rows);  

      client.query(`INSERT INTO Reviews (version, "approvalStatus", "reviewText", "publishingDate", "funFact", vote, "productSku", "userId", rid)
          VALUES('0', 'approved', 'this is just a test', '2023-03-03', 'please work', '0', 'd63h57d738s1', '2', 'R11111111')`,
           (err, res) => {
            if (err) {
              console.error('Error performing query:', err.stack);
            } else {
              console.log('Query result:', res.rows);
            }
           });
    }
  })

  //Votes table
  client.query(`CREATE TABLE IF NOT EXISTS Votes (
    "voteID" SERIAL PRIMARY KEY,
    vid VARCHAR(255) NOT NULL,
    vote VARCHAR(255) NOT NULL,
    rid VARCHAR(255) NOT NULL,
    "userID" BIGINT NOT NULL
  ) `, (err, res) => { 
    if (err) {
      console.error('Error performing query:', err.stack);
    } else {
      console.log('Query result:', res.rows);  

      client.query(`INSERT INTO Votes (vid, vote, rid, "userID") VALUES ('V11111111', 'upVote', 'R11111111', '2')`, (err, res) => {
        if (err) {
          console.error('Error performing query:', err.stack);
        } else {
          console.log('Query result:', res.rows);
        }
      })
    }
  }) 


  createQueueAndListener();
  createRPCQueueAndListener();
}

async function createRPCQueueAndListener() {
  try {
    // Connect to RabbitMQ server
    const connection = await amqp.connect('amqp://rabbitmq');
    // Create a channel
    const channel = await connection.createChannel();
    const exchangeName = 'rpc_reviews_exchange'; // replace with your exchange name
    const exchangeOptions = { durable: true }; // replace with your exchange options
    await channel.assertExchange(exchangeName, 'direct', exchangeOptions);
    // Set up a queue to receive messages
    const queue = 'rpc_reviews_queue';

    await channel.assertQueue(queue, { durable: true });
    channel.bindQueue(queue, exchangeName, "");
    channel.prefetch(1);
    console.log('Waiting for RPC requests...');

    channel.consume(queue, async function (msg) {
      console.log('Received RPC request');
      if(msg.content.toString() === 'getProducts') {
        console.log('Received RPC request for getProducts');
        // Get all products from the database
      client.query('SELECT sku, designation, description FROM Products', (err, res) => {
        if (err) {
          console.error('Error performing query:', err.stack);
        } else {
          console.log('Query result:', res.rows);
          const payload = JSON.stringify(res.rows);
          // Send the response to the RPC client
          channel.sendToQueue(msg.properties.replyTo,
            Buffer.from(payload), {
              correlationId: msg.properties.correlationId
            });
        }});

      channel.ack(msg);
      } else if (msg.content.toString() === 'getReviews') {
        console.log('Received RPC request for getReviews');
        // Get all reviews from the database
        client.query('SELECT * FROM Reviews', (err, res) => {
          if (err) {
            console.error('Error performing query:', err.stack);
          } else {
            console.log('Query result:', res.rows);
            
            const payload = JSON.stringify(res.rows);
            console.log(payload);
            // Send the response to the RPC client
            channel.sendToQueue(msg.properties.replyTo,
              Buffer.from(payload), {
                correlationId: msg.properties.correlationId
              });
          }});
          channel.ack(msg);
        } else if (msg.content.toString() === 'getVotes') {
          console.log('Received RPC request for getVotes');
          // Get all votes from the database
          client.query('SELECT * FROM Votes', (err, res) => {
            if (err) {
              console.error('Error performing query:', err.stack);
            } else {
              console.log('Query result:', res.rows);
              const payload = JSON.stringify(res.rows);
              // Send the response to the RPC client
              channel.sendToQueue(msg.properties.replyTo,
                Buffer.from(payload), {
                  correlationId: msg.properties.correlationId
                });
            }});
            channel.ack(msg);
          } else {
            console.log('Type of RPC request not recognized')
        }
    })
  } catch (error) {
    console.error(error);
  }
}

async function createQueueAndListener() {
  try {
    // Connect to RabbitMQ server
    const connection = await amqp.connect('amqp://rabbitmq');
    // Create a channel
    const channel = await connection.createChannel();
    
    // Create a queue
    const queueName = 'reviews_bootstraper_queue';
    await channel.assertQueue(queueName);
    channel.bindQueue(queueName, "fanout_exchange", "");
    // Set up a listener to consume messages from the queue
    channel.consume(queueName, (msg) => {
      console.log(`Received message: ${msg.content.toString()}`);
      const object = JSON.parse(msg.content);
      console.log(object.sku);
      switch (msg.properties.headers.action) {
        case 'product-created':
          client.query(`INSERT INTO Products (sku, designation, description) 
          VALUES('${object.sku}', '${object.designation}', '${object.description}')`,
            (err, res) => {
              if (err) {
                console.error('Error performing query:', err.stack);
              } else {
                console.log('Query result:', res.rows);
              }
            });
            break;
        case 'product-updated':
          client.query(`UPDATE Products
          SET designation='${object.designation}', description='${object.description}'
          WHERE sku='${object.sku}';
          `,
            (err, res) => {
              if (err) {
                console.error('Error performing query:', err.stack);
              } else {
                console.log('Query result:', res.rows);
              }
            });
            break;

        case 'product-deleted':
          client.query(`DELETE FROM Products
          WHERE sku='${object.sku}';
          `,
            (err, res) => {
              if (err) {
                console.error('Error performing query:', err.stack);
              } else {
                console.log('Query result:', res.rows);
              }
            });
            break;
        case 'review-created':
          const dateObject = new Date(object.publishingDate[0], object.publishingDate[1] - 1, object.publishingDate[2]);
          const localDateString = dateObject.toISOString().substring(0, 10);
          console.log(localDateString);
          client.query(`INSERT INTO Reviews (version, "approvalStatus", "reviewText", "publishingDate", "funFact", vote, "productSku", "userId", rid)
          VALUES('${object.version}', '${object.approvalStatus}', '${object.reviewText}', '${localDateString}', '${object.funFact}', '${object.vote}', '${object.productSku}', '${object.userId}', '${object.rid}')`,
           (err, res) => {
            if (err) {
              console.error('Error performing query:', err.stack);
            } else {
              console.log('Query result:', res.rows);
            }
           });
          break;

        case 'review-moderated':
          client.query(`UPDATE Reviews SET version='${object.version}', "approvalStatus"='${object.approvalStatus}'
          WHERE rid='${object.rid}'`, (err, res) => {
            if (err) {
              console.error('Error performing query:', err.stack);
            } else {
              console.log('Query result:', res.rows);
            }
          });
          break;
           
        case 'review-deleted':
          client.query(`DELETE FROM Reviews WHERE rid='${object.rid}'`, (err, res) => { 
            if (err) {
              console.error('Error performing query:', err.stack);
            } else {
              console.log('Query result:', res.rows);
            }
          });
          break;
        case 'vote-header':
          client.query(`INSERT INTO Votes (vid, vote, "rid", "userID")
          VALUES('${object.vid}', '${object.vote}', '${object.rid}', '${object.userID}')`, (err, res) => {  
            if (err) {
              console.error('Error performing query:', err.stack);
            } else {
              console.log('Query result:', res.rows);
            }
          });
          break;
        default:
          break;
      }

      // Acknowledge that the message has been processed
      channel.ack(msg);
    });
  } catch (error) {
    console.error(error);
  }
}

main().catch(console.error);