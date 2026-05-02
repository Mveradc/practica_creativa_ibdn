// Conectar con WebSocket
const socket = io();

// Guardar la solicitud activa
let currentRequestId = null;
const predictionLabels = {
  0: "Early (15+ Minutes Early)",
  1: "Slightly Early (0-15 Minute Early)",
  2: "Slightly Late (0-30 Minute Delay)",
  3: "Very Late (30+ Minutes Late)"
};

// Cuando el formulario se envíe
$("#flight_delay_classification").submit(function(event) {
  event.preventDefault();
  
  var $form = $(this);
  var url = $form.attr("action");
  
  // Enviar POST con los datos
  var payload = $form.serializeArray();
  var posting = $.post(
    url,
    payload
  );
  
  // Guardar el UUID de respuesta
  posting.done(function(data) {
    var response = JSON.parse(data);
    
    if(response.status == "OK") {
      currentRequestId = response.id;
      $("#result").empty().append("Processing...");
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
  const displayMessage = predictionLabels[String(response.Prediction)] || "";
  $("#result").empty().append(displayMessage);
}