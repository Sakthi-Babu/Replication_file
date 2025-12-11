using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Renci.SshNet;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace CMDDSReplication.Services
{
    public class ScpService
    {
        private readonly ILogger<ScpService> _logger;
        private readonly string _targetHost;
        private readonly string _targetUser;
        private readonly string _targetBasePath;
        private readonly int _maxRetries;
        private readonly int _retryIntervalSeconds;

        public ScpService(ILogger<ScpService> logger)
        {
            _logger = logger;
            _targetHost = Environment.GetEnvironmentVariable("TARGET_VMSS_IP")
                ?? throw new ArgumentNullException("TARGET_VMSS_IP not configured");
            _targetUser = Environment.GetEnvironmentVariable("TARGET_VMSS_USER") ?? "cmdds";
            _targetBasePath = Environment.GetEnvironmentVariable("TARGET_BASE_PATH") ?? "/mnt/cmdds/files";
            _maxRetries = int.Parse(Environment.GetEnvironmentVariable("MAX_RETRIES") ?? "3");
            _retryIntervalSeconds = int.Parse(Environment.GetEnvironmentVariable("RETRY_INTERVAL_SECONDS") ?? "10");
        }

        public async Task TransferFileAsync(string sourceFilePath, string fileName)
        {
            var targetPath = Path.Combine(_targetBasePath, fileName);
            var attempt = 0;
            Exception? lastException = null;

            while (attempt < _maxRetries)
            {
                attempt++;
                try
                {
                    _logger.LogInformation($"SCP attempt {attempt}/{_maxRetries}: {fileName} → {_targetHost}:{targetPath}");

                    // Load SSH private key from environment variable (base64 encoded)
                    var sshKeyBase64 = Environment.GetEnvironmentVariable("SSH_PRIVATE_KEY_BASE64")
                        ?? throw new ArgumentNullException("SSH_PRIVATE_KEY_BASE64 not configured");

                    var sshKeyBytes = Convert.FromBase64String(sshKeyBase64);
                    var sshKeyContent = Encoding.UTF8.GetString(sshKeyBytes);

                    // Write key to temp file (required by SSH.NET)
                    var tempKeyPath = Path.Combine(Path.GetTempPath(), $"ssh_key_{Guid.NewGuid()}.pem");
                    await File.WriteAllTextAsync(tempKeyPath, sshKeyContent);

                    try
                    {
                        var keyFile = new PrivateKeyFile(tempKeyPath);
                        var connectionInfo = new Renci.SshNet.ConnectionInfo(
                            _targetHost,
                            22,
                            _targetUser,
                            new PrivateKeyAuthenticationMethod(_targetUser, keyFile)
                        );

                        connectionInfo.Timeout = TimeSpan.FromMinutes(5);

                        await Task.Run(() =>
                        {
                            using (var client = new ScpClient(connectionInfo))
                            {
                                client.OperationTimeout = TimeSpan.FromMinutes(10);
                                client.Connect();

                                using (var fileStream = File.OpenRead(sourceFilePath))
                                {
                                    client.Upload(fileStream, targetPath);
                                }

                                client.Disconnect();
                            }
                        });

                        _logger.LogInformation($"✅ SCP transfer successful: {fileName}");
                        return;
                    }
                    finally
                    {
                        // Clean up temp key file
                        if (File.Exists(tempKeyPath))
                        {
                            File.Delete(tempKeyPath);
                        }
                    }
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    _logger.LogWarning(ex, $"⚠️ SCP attempt {attempt} failed for {fileName}");

                    if (attempt < _maxRetries)
                    {
                        _logger.LogInformation($"Retrying in {_retryIntervalSeconds} seconds...");
                        await Task.Delay(TimeSpan.FromSeconds(_retryIntervalSeconds));
                    }
                }
            }

            throw new Exception($"SCP transfer failed after {_maxRetries} attempts for {fileName}", lastException);
        }
    }
}