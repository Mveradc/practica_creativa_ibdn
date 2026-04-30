// Conectar con WebSocket
const socket = io();

// Guardar el UUID de esta solicitud
let currentRequestId = null;

// Cuando el formulario se envíe
$("#flight_delay_classification").submit(function(event) {
  event.preventDefault();
  
  var $form = $(this);
  var url = $form.attr("action");
  var requestId = (window.crypto && crypto.randomUUID) ? crypto.randomUUID() : String(Date.now()) + String(Math.random()).slice(2);
  currentRequestId = requestId;
  
  // Enviar POST con los datos
  var payload = $form.serializeArray();
  payload.push({ name: "UUID", value: requestId });
  var posting = $.post(
    url,
    payload
  );
  
  // Guardar el UUID de respuesta
  posting.done(function(data) {
    var response = JSON.parse(data);
    
    if(response.status == "OK") {
      $("#result").empty().append("Esperando predicción...");
      console.log("Solicitud enviada con UUID: " + currentRequestId);
    }
  });
});

// Escuchar predicciones del servidor
socket.on('new_prediction', function(prediction) {
  console.log("Predicción recibida:", prediction);
  
  // Solo procesar si es para esta solicitud
  if(prediction.UUID === currentRequestId) {
    renderPage(prediction);
  }
});

// Renderizar resultado
function renderPage(response) {
  console.log(response);
  
  var displayMessage;
  
  if(response.Prediction == 0 || response.Prediction == '0') {
    displayMessage = "Early (15+ Minutes Early)";
  }
  else if(response.Prediction == 1 || response.Prediction == '1') {
    displayMessage = "Slightly Early (0-15 Minute Early)";
  }
  else if(response.Prediction == 2 || response.Prediction == '2') {
    displayMessage = "Slightly Late (0-30 Minute Delay)";
  }
  else if(response.Prediction == 3 || response.Prediction == '3') {
    displayMessage = "Very Late (30+ Minutes Late)";
  }
  
  console.log(displayMessage);
  $("#result").empty().append(displayMessage);
}

// Conectar/desconectar
socket.on('connect', function() {
  console.log('Conectado al servidor WebSocket');
});

socket.on('disconnect', function() {
  console.log('Desconectado del servidor');
});