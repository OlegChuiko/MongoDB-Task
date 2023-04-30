const https = require('https');
const zlib = require('zlib');
const readline = require('readline');
const { MongoClient } = require('mongodb');

// URL для завантаження архіву файлу з даними про фільми
const URL = 'https://popwatch-staging.s3.us-east-2.amazonaws.com/movies_1.gz';

// З'єднання з базою даних MongoDB
const CLIENT = new MongoClient('mongodb+srv://MyBdUser:oleg2005chuiko@cluster0.ijnvwvb.mongodb.net/?retryWrites=true&w=majority');

// Функція для додавання одного фільму до бази даних MongoDB
async function AddData(data, collection) 
{
  await collection.insertOne(data);
  console.log('Adding data...');
}

// Асинхронна функція для завантаження файлу, розпакування та збереження даних в MongoDB
async function LoadMovies() 
{
  try 
  {
    await CLIENT.connect();
    const collection = CLIENT.db('movies_db').collection('movies_collection');

    // Завантаження архіву файлу з даними про фільми
    const zip = zlib.createGunzip();

    const request = https.get(URL, (response) => 
    {
      response.pipe(zip);

      // Розбиття отриманого файлу на окремі JSON-об'єкти
      const rl = readline.createInterface(
      {
        input: zip,
        crlfDelay: Infinity
      });

      // Обробка кожного JSON-об'єкта та додавання його до колекції MongoDB
      rl.on('line', async (line) => 
      {
        const movie = JSON.parse(line);
        await AddData(movie, collection);
      });

      rl.on('close', () => 
      {
        CLIENT.close();
      });

    });

    request.on('error', (error) => 
    {
      console.error(error);
    });
  }
  catch (error) 
  {
    console.error(error);
  }
}

//Запуск функції
LoadMovies();