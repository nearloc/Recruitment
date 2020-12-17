namespace nearloc.isealim.mqtt.broker.console
{
    using MQTTnet;
    using MQTTnet.Protocol;
    using MQTTnet.Server;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Schema;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Mail;
    using System.Runtime.InteropServices;
    using System.Security.Cryptography;
    using System.Text;
    using System.Threading.Tasks;
    class Program
    {
        #region Trap application termination
        static bool exitSystem = false;
        [DllImport("Kernel32")]
        private static extern bool SetConsoleCtrlHandler(EventHandler handler, bool add);

        private delegate bool EventHandler(CtrlType sig);
        static EventHandler _handler;

        enum CtrlType
        {
            CTRL_C_EVENT = 0,
            CTRL_BREAK_EVENT = 1,
            CTRL_CLOSE_EVENT = 2,
            CTRL_LOGOFF_EVENT = 5,
            CTRL_SHUTDOWN_EVENT = 6
        }
        private static bool Handler(CtrlType sig)
        {
            Console.WriteLine("Exiting");

            var applicantsJson = JsonConvert.SerializeObject(_applicants);
            var ipListJson = JsonConvert.SerializeObject(_ipList);

            File.WriteAllText(_config.ApplicationsJsonFile, applicantsJson);
            File.WriteAllText(_config.IpListJsonFile, ipListJson);

            Console.WriteLine("Done saving lists");

            //allow main to run off
            exitSystem = true;

            //shutdown right away so there are no lingering threads
            Environment.Exit(-1);

            return true;
        }
        #endregion

        private const string IsAdminSessionKey = "IsAdmin";
        private const string IpSessionKey = "Ip";
        private const string ApplicationTopic = "nearloc/application";
        private const int MinimumMessageLength = 100;
        private const int MaximumMessageLength = 350;

        private static Config _config;

        private static List<Applicant> _applicants = new List<Applicant>();

        private static List<Tuple<string, DateTime>> _ipList = new List<Tuple<string, DateTime>>();

        //known obsolete
        private static readonly JsonSchema _schema = JsonSchema.Parse(File.ReadAllText("messageSchema.json"));

        static void Main(string[] args)
        {
            ReadConfig();

            #region Trap Application termination
            // Some boilerplate to react to close window event, CTRL-C, kill, etc
            _handler += new EventHandler(Handler);
            SetConsoleCtrlHandler(_handler, true);
            #endregion            

            CreateApplicationsDirectoryIfNotExists();

            FillApplicantsList();

            FillIpList();

            var optionsBuilder = new MqttServerOptionsBuilder()
                .WithDefaultEndpoint()
                .WithDefaultEndpointPort(_config.MqttPort)
                .WithConnectionValidator(context =>
                {
                    //when connection requested

                    var user = _config.Users.FirstOrDefault(u => u.Name == context.Username);
                    if (user == null)
                    {
                        context.ReasonCode = MqttConnectReasonCode.BadUserNameOrPassword;
                        return;
                    }

                    var hash = ComputeSha256Hash(context.Password);

                    if (!string.Equals(user.Hash, hash))
                    {
                        context.ReasonCode = MqttConnectReasonCode.BadUserNameOrPassword;
                        return;
                    }

                    context.SessionItems.Add(IsAdminSessionKey, user.IsAdmin);
                    context.SessionItems.Add(IpSessionKey, context.Endpoint.Split(":")[0]);
                    context.ReasonCode = MqttConnectReasonCode.Success;

                    Console.WriteLine($"{context.Username} connected {context.Endpoint}");

                }).WithSubscriptionInterceptor(context =>
                {
                    //when subscription requested

                    if (!context.SessionItems.TryGetValue(IsAdminSessionKey, out var isAdminObject))
                    {
                        context.AcceptSubscription = false;
                        return;
                    }

                    var isAdmin = (bool)isAdminObject;

                    if (!isAdmin)
                    {
                        context.AcceptSubscription = false;
                        return;
                    }

                    context.AcceptSubscription = true;

                    Console.WriteLine("Admin subscribed to application topic");
                })
                .WithApplicationMessageInterceptor(context =>
                {
                    //when message published

                    if (!context.SessionItems.TryGetValue(IpSessionKey, out var ipObject))
                    {
                        context.AcceptPublish = false;
                        return;
                    }

                    var ip = (string)ipObject;

                    if (!IsIpAllowedToPublish(ip))
                    {
                        context.AcceptPublish = false;
                        return;
                    }
                    else
                    {
                        _ipList.RemoveAll(t => t.Item1 == ip);
                    }

                    _ipList.Add(new Tuple<string, DateTime>(ip, DateTime.Now));

                    if (context.ApplicationMessage == null 
                    || context.ApplicationMessage.Payload == null 
                    || context.ApplicationMessage.Payload.Length < MinimumMessageLength
                    || context.ApplicationMessage.Payload.Length > MaximumMessageLength)
                    {
                        context.AcceptPublish = false;
                        return;
                    }

                    if (!string.Equals(context.ApplicationMessage.Topic, ApplicationTopic))
                    {
                        context.AcceptPublish = false;
                        return;
                    }

                    var json = Encoding.UTF8.GetString(context.ApplicationMessage.Payload);
                    if (!IsValidJson(json))
                    {
                        context.AcceptPublish = false;
                        return;
                    }

                    var applicant = JsonConvert.DeserializeObject<Applicant>(json);
                    if (_applicants.Any(a => a.FullName == applicant.FullName) || _applicants.Any(a => a.LinkToResume == applicant.LinkToResume))
                    {
                        context.AcceptPublish = false;
                        return;
                    }

                    _applicants.Add(applicant);

                    Task.Run(() => SavePayload(context.ApplicationMessage.Payload));

                    Task.Run(() => SendEmail(applicant));

                    context.AcceptPublish = true;

                    Console.WriteLine($"Message received from {context.ClientId}");

                })
                .Build();

            /*
             * you have found the brokers ip address, do you have the applicant password?
             * 
                 _  ___   _  ___ _____ _  ___   __    _ _ ____  
                / |( _ ) / |/ _ \___  / |( _ ) / /_  / / |___ \ 
                | |/ _ \ | | (_) | / /| |/ _ \| '_ \ | | | __) |
                | | (_) || |\__, |/ /_| | (_) | (_) || | |/ __/ 
                |_|\___(_)_|  /_//_/(_)_|\___/ \___(_)_|_|_____|
            *
            */

            var mqttServer = new MqttFactory().CreateMqttServer();
            mqttServer.StartAsync(optionsBuilder);

            Console.WriteLine("Broker started");

            Console.ReadLine();
        }

        private static bool IsIpAllowedToPublish(string ip)
        {
            var ipRecord = _ipList.FirstOrDefault(r => r.Item1 == ip);
            if (ipRecord != null)
            {
                var nextDateTime = ipRecord.Item2.AddHours(_config.IpHourLimit);
                return nextDateTime < DateTime.Now;
            }

            return true;
        }

        private static void SavePayload(byte[] payload)
        {
            var fileName = $"{DateTime.Now.ToString("yyyyMMddHHmmssfff")}_{Guid.NewGuid()}.json";
            var filePath = Path.Combine(_config.ApplicationsDirectory, fileName);

            File.WriteAllBytes(filePath, payload);
        }

        private static void SendEmail(Applicant applicant)
        {
            try
            {
                using (SmtpClient client = new SmtpClient(_config.MailConfig.Host, _config.MailConfig.Port))
                {
                    var sender = new MailAddress(_config.MailConfig.SenderMailAccount, _config.MailConfig.SenderDisplayName);
                    MailMessage mail = new MailMessage();
                    mail.From = sender;
                    mail.Priority = MailPriority.High;
                    mail.Bcc.Add(sender);
                    mail.Subject = _config.MailConfig.Subject;
                    mail.To.Add(new MailAddress(applicant.Email, applicant.FullName));
                    mail.Body = File.ReadAllText(_config.MailConfig.TemplatePath)
                        .Replace("{FullName}", applicant.FullName)
                        .Replace("{LinkToResume}", applicant.LinkToResume)
                        .Replace("{Phone}", applicant.Phone);
                    mail.IsBodyHtml = false;

                    NetworkCredential girisIzni = new NetworkCredential(_config.MailConfig.SenderMailAccount, _config.MailConfig.Password);
                    client.UseDefaultCredentials = false;
                    client.EnableSsl = true;
                    client.Credentials = girisIzni;

                    client.Send(mail);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        private static void ReadConfig()
        {
            var json = File.ReadAllText("config.json");
            _config = JsonConvert.DeserializeObject<Config>(json);
        }

        private static void FillApplicantsList()
        {
            if (File.Exists(_config.ApplicationsJsonFile))
            {
                var applicantsJson = File.ReadAllText(_config.ApplicationsJsonFile);
                _applicants = JsonConvert.DeserializeObject<List<Applicant>>(applicantsJson);
            }
        }

        private static void FillIpList()
        {
            if (File.Exists(_config.IpListJsonFile))
            {
                var ipListJson = File.ReadAllText(_config.IpListJsonFile);
                _ipList = JsonConvert.DeserializeObject<List<Tuple<string, DateTime>>>(ipListJson);
            }
        }

        private static string ComputeSha256Hash(string input)
        {
            using (SHA256 sha256Hash = SHA256.Create())
            {
                byte[] bytes = sha256Hash.ComputeHash(Encoding.UTF8.GetBytes(input));

                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < bytes.Length; i++)
                {
                    builder.Append(bytes[i].ToString("x2"));
                }
                return builder.ToString();
            }
        }

        private static bool IsValidJson(string json)
        {
            try
            {
                var token = Newtonsoft.Json.Linq.JToken.Parse(json);

                IList<string> errorMessages;
                return token.IsValid(_schema, out errorMessages);
            }
            catch
            {
                return false;
            }
        }

        private static void CreateApplicationsDirectoryIfNotExists()
        {
            if (!Directory.Exists(_config.ApplicationsDirectory))
            {
                Directory.CreateDirectory(_config.ApplicationsDirectory);
            }
        }
    }
}

