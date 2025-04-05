using System;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace MQClientLibrary
{
    public class Topic
    {
        public string Name { get; }
        public Topic(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("El nombre del tema no puede estar vacío.");
            Name = name;
        }
    }

    public class Message
    {
        public string Content { get; }
        public Message(string content)
        {
            if (string.IsNullOrWhiteSpace(content))
                throw new ArgumentException("El contenido del mensaje no puede estar vacío.");
            Content = content;
        }
    }

    public class MQClient
    {
        private readonly string _ip;
        private readonly int _port;
        private readonly Guid _appId;

        public MQClient(string ip, int port, Guid appId)
        {
            if (string.IsNullOrWhiteSpace(ip))
                throw new ArgumentException("La dirección IP no puede estar vacía.");
            if (port <= 0)
                throw new ArgumentException("El puerto debe ser un número positivo.");

            _ip = ip;
            _port = port;
            _appId = appId;
        }

        private MessageResponse SendRequest(object request)
        {
            try
            {
                using TcpClient client = new TcpClient(_ip, _port);
                using NetworkStream stream = client.GetStream();

                string jsonRequest = JsonSerializer.Serialize(request);
                byte[] data = Encoding.UTF8.GetBytes(jsonRequest);
                stream.Write(data, 0, data.Length);

                byte[] responseBuffer = new byte[4096];
                int bytesRead = stream.Read(responseBuffer, 0, responseBuffer.Length);
                string jsonResponse = Encoding.UTF8.GetString(responseBuffer, 0, bytesRead);

                return JsonSerializer.Deserialize<MessageResponse>(jsonResponse);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error en la comunicación con el broker: {ex.Message}");
            }
        }

        public bool Subscribe(Topic topic)
        {
            var request = new { Action = "Subscribe", AppId = _appId, Topic = topic.Name };
            return SendRequest(request)?.Success ?? false;
        }

        public bool Unsubscribe(Topic topic)
        {
            var request = new { Action = "Unsubscribe", AppId = _appId, Topic = topic.Name };
            return SendRequest(request)?.Success ?? false;
        }

        public bool Publish(Message message, Topic topic)
        {
            var request = new { Action = "Publish", AppId = _appId, Topic = topic.Name, Content = message.Content };
            return SendRequest(request)?.Success ?? false;
        }

        public Message Receive(Topic topic)
        {
            var request = new { Action = "Receive", AppId = _appId, Topic = topic.Name };
            var response = SendRequest(request);

            if (response?.Success == true && !string.IsNullOrEmpty(response.Content))
                return new Message(response.Content);

            throw new InvalidOperationException("No se recibió un mensaje válido del broker.");
        }
    }

    public class MessageResponse
    {
        public bool Success { get; set; }
        public string Message { get; set; }
        public string Content { get; set; }
    }
}
