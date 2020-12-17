namespace nearloc.isealim.mqtt.broker.console
{
    using System.Collections.Generic;
    public class Config
    {
        public int IpHourLimit { get; set; }
        public string ApplicationsDirectory { get; set; }
        public string ApplicationsJsonFile { get; set; }
        public string IpListJsonFile { get; set; }
        public int MqttPort { get; set; }
        public List<User> Users { get; set; }
        public MailConfig MailConfig { get; set; }
    }

    public class MailConfig
    {
        public string SenderMailAccount { get; set; }
        public string Password { get; set; }
        public string SenderDisplayName { get; set; }
        public int Port { get; set; }
        public string Host { get; set; }
        public string Subject { get; set; }
        public string TemplatePath { get; set; }
    }
}

