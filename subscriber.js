const express = require('express');
const amqp = require('amqplib/callback_api');
const cors = require('cors');
const { MongoClient } = require('mongodb');
const geolib = require('geolib');
require('dotenv').config();

const app = express();
const port = 3000;

const rabbitMqUrl = process.env.AMQP_URL;
const mongoUri = process.env.MONGO_URI;
const dbName = 'AttendEase';
const attendanceCollectionName = 'attendanceDetails';
const officeCollectionName = 'officeDetails';
const userDetailsCollectionName = 'userDetails';

app.use(cors());

async function isWithinRange(userLocation) {
    const client = new MongoClient(mongoUri, { useNewUrlParser: true, useUnifiedTopology: true });
    let nearestOffice = null;

    try {
        await client.connect();
        console.log('Connected to MongoDB Atlas');

        const database = client.db(dbName);
        const officeCollection = database.collection(officeCollectionName);

        const offices = await officeCollection.find({}).toArray();

        for (const office of offices) {
            const officeLocation = {
                latitude: office.latitude,
                longitude: office.longitude
            };

            const distance = geolib.getDistance(userLocation, officeLocation);

            if (distance <= 200) {
                nearestOffice = office;
                break;
            }
        }
    } catch (error) {
        console.error('Error connecting to MongoDB Atlas:', error);
    } finally {
        await client.close();
    }

    return nearestOffice;
}

async function checkUserDetails(userDetails) {
    const client = new MongoClient(mongoUri, { useNewUrlParser: true, useUnifiedTopology: true });

    try {
        await client.connect();
        console.log('Connected to MongoDB Atlas');

        const database = client.db(dbName);
        const userCollection = database.collection(userDetailsCollectionName);

        const existingUser = await userCollection.findOne({ userid: userDetails.userid });


        if (existingUser) {
            const detailsMatch = (
                existingUser.osName === userDetails.osName &&
                existingUser.osVersion === userDetails.osVersion &&
                existingUser.brand === userDetails.brand &&
                existingUser.model === userDetails.model &&
                existingUser.manufacturer === userDetails.manufacturer
            );

            return detailsMatch;
        }
    } catch (error) {
        console.error('Error connecting to MongoDB Atlas:', error);
    } finally {
        await client.close();
    }

    return false;
}


async function insertDataToMongo(data, officeDetails) {
    const client = new MongoClient(mongoUri, { useNewUrlParser: true, useUnifiedTopology: true });

    try {
        await client.connect();
        console.log('Connected to MongoDB Atlas');

        const database = client.db(dbName);
        const collection = database.collection(attendanceCollectionName);

        const currentDate = new Date();
        const formattedData = {
            ...data,
            date: currentDate.toISOString().split('T')[0],
            time: currentDate.toTimeString().split(' ')[0],
            officeDetails
        };

        const result = await collection.insertOne(formattedData);
        console.log('Data inserted into MongoDB:', result.insertedId);
    } catch (error) {
        console.error('Error connecting to MongoDB Atlas:', error);
    } finally {
        await client.close();
    }
}

amqp.connect(rabbitMqUrl, (err, connection) => {
    if (err) {
        throw err;
    }

    connection.createChannel((err, channel) => {
        if (err) {
            throw err;
        }

        const queueName = 'attendease-queue';
        channel.assertQueue(queueName, { durable: false });

        channel.consume(queueName, async (msg) => {
            const messageContent = msg.content.toString();
            console.log(`Message received: ${messageContent}`);

            try {
                const data = JSON.parse(messageContent);
                const userLocation = { latitude: data.latitude, longitude: data.longitude };

                const userDetailsMatch = await checkUserDetails({
                    userid: data.userid,
                    brand: data.brand,
                    model: data.model,
                    osName: data.osName,
                    osVersion: data.osVersion,
                    manufacturer: data.manufacturer
                });

                if (userDetailsMatch) {
                    const nearestOffice = await isWithinRange(userLocation);

                    if (nearestOffice) {
                        await insertDataToMongo(data, nearestOffice);
                        console.log('Data saved to MongoDB as user location is within 200 meters and user details match.');
                    } else {
                        console.log('User location is not within 200 meters of any office. Data not saved.');
                    }
                } else {
                    console.log('User details do not match. Data not saved.');
                }
            } catch (error) {
                console.error('Error processing message:', error);
            } finally {
                channel.ack(msg);
            }
        }, {
            noAck: false
        });

        console.log(`Waiting for messages in ${queueName}. To exit press CTRL+C`);
    });
});

app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});
