const express = require('express');
const app = express();
const PORTA = 21069;

app.use(express.json()); // So Express know you're using JSON

app.get('/', (request, response) => {
  return 'Hello World!';
});

// POST a JSON object and get it back
app.post('/demo-object', (request, response) => {

  const body = request.body;

  return body;
});

// GET with the id and get it back
app.get('/demo-object/:id', (request, response) => {
  
  const params = request.params;

  return response.json(params);
});

// GET with query in the URI and get it back
app.get('/demo-object', (request, response) => {
  
  const params = request.query;

  return response.json(query);
});

app.listen(PORTA);