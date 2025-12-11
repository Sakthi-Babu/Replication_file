using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using CMDDSReplication.Services;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;


namespace CMDDSReplication.Functions
{
    public class FileMonitoringFunction
    {
        private readonly ILogger _logger;
        private readonly ScpService _scpService;
        private readonly DatabaseService _databaseService;
        private readonly string _mountPoint;
        private readonly string _sourceRegion;
        private readonly string _targetRegion;
        private static HashSet<string> _processedFiles = new HashSet<string>();

        public FileMonitoringFunction(
            ILoggerFactory loggerFactory,
            ScpService scpService,
            DatabaseService databaseService)
        {
            _logger = loggerFactory.CreateLogger<FileMonitoringFunction>();
            _scpService = scpService;
            _databaseService = databaseService;
            _mountPoint = Environment.GetEnvironmentVariable("MOUNT_POINT") ?? "/mnt/cmdds/files";
            _sourceRegion = Environment.GetEnvironmentVariable("SOURCE_REGION") ?? "EastUS2";
            _targetRegion = Environment.GetEnvironmentVariable("TARGET_REGION") ?? "CentralUS";
        }

        [Function("FileMonitoring")]
        public async Task Run([TimerTrigger("*/10 * * * * *")] TimerInfo myTimer)
        {
            _logger.LogInformation($"[{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}] File monitoring triggered");

            try
            {
                // Verify mount point exists
                if (!Directory.Exists(_mountPoint))
                {
                    _logger.LogError($"Mount point does not exist: {_mountPoint}");
                    return;
                }

                // Scan for new files
                var files = Directory.GetFiles(_mountPoint, "*.*", SearchOption.TopDirectoryOnly)
                    .Where(f => !_processedFiles.Contains(f))
                    .ToList();

                if (files.Count == 0)
                {
                    _logger.LogInformation("No new files detected");
                    return;
                }

                _logger.LogInformation($"Detected {files.Count} new file(s) for replication");

                // Process each file (uni-directional: East US2 → Central US)
                foreach (var filePath in files)
                {
                    try
                    {
                        await ProcessFile(filePath);
                        _processedFiles.Add(filePath);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"Failed to process file: {filePath}");
                    }
                }

                // Cleanup processed files cache if too large
                if (_processedFiles.Count > 10000)
                {
                    _processedFiles.Clear();
                    _logger.LogInformation("Cleared processed files cache");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "File monitoring failed");
            }
        }

        private async Task ProcessFile(string filePath)
        {
            var fileName = Path.GetFileName(filePath);
            var fileInfo = new FileInfo(filePath);
            var fileSize = fileInfo.Length;
            var uploadTime = fileInfo.CreationTimeUtc;

            _logger.LogInformation($"Processing: {fileName} ({fileSize} bytes)");

            // Insert queue record
            var queueId = await _databaseService.InsertReplicationQueueAsync(
                fileName,
                fileSize,
                _sourceRegion,
                _targetRegion,
                "Pending",
                uploadTime
            );

            var startTime = DateTime.UtcNow;

            try
            {
                // Transfer file via SCP (SSH)
                await _scpService.TransferFileAsync(filePath, fileName);

                var endTime = DateTime.UtcNow;
                var duration = (int)(endTime - startTime).TotalMilliseconds;

                // Update queue status
                await _databaseService.UpdateReplicationQueueAsync(
                    queueId,
                    "Completed",
                    startTime,
                    endTime
                );

                // Insert audit record
                await _databaseService.InsertReplicationAuditAsync(
                    fileName,
                    fileSize,
                    "Completed",
                    _sourceRegion,
                    _targetRegion,
                    uploadTime,
                    endTime,
                    duration,
                    null
                );

                _logger.LogInformation($"✅ Successfully replicated: {fileName} ({duration}ms)");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"❌ Replication failed: {fileName}");

                var endTime = DateTime.UtcNow;
                var duration = (int)(endTime - startTime).TotalMilliseconds;

                // Update queue with failure
                await _databaseService.UpdateReplicationQueueAsync(
                    queueId,
                    "Failed",
                    startTime,
                    endTime
                );

                // Insert audit record with error
                await _databaseService.InsertReplicationAuditAsync(
                    fileName,
                    fileSize,
                    "Failed",
                    _sourceRegion,
                    _targetRegion,
                    uploadTime,
                    endTime,
                    duration,
                    ex.Message
                );

                throw;
            }
        }
    }
}