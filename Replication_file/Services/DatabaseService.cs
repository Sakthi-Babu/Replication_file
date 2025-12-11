using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Azure.Identity;
using System;
using System.Threading.Tasks;

namespace CMDDSReplication.Services
{
    public class DatabaseService
    {
        private readonly ILogger<DatabaseService> _logger;
        private readonly string _connectionString;

        public DatabaseService(ILogger<DatabaseService> logger)
        {
            _logger = logger;
            _connectionString = Environment.GetEnvironmentVariable("AZURE_SQL_CONNECTION_STRING")
                ?? throw new ArgumentNullException("AZURE_SQL_CONNECTION_STRING not configured");
        }

        public async Task<long> InsertReplicationQueueAsync(
            string fileName,
            long fileSize,
            string sourceRegion,
            string targetRegion,
            string status,
            DateTime uploadTime)
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            await using var command = new SqlCommand(@"
                INSERT INTO FileReplicationQueue 
                    (FileName, FileSize, SourceRegion, TargetRegion, Status, UploadTime, CreatedDate, ModifiedDate)
                VALUES 
                    (@FileName, @FileSize, @SourceRegion, @TargetRegion, @Status, @UploadTime, GETUTCDATE(), GETUTCDATE());
                SELECT CAST(SCOPE_IDENTITY() as bigint);
            ", connection);

            command.Parameters.AddWithValue("@FileName", fileName);
            command.Parameters.AddWithValue("@FileSize", fileSize);
            command.Parameters.AddWithValue("@SourceRegion", sourceRegion);
            command.Parameters.AddWithValue("@TargetRegion", targetRegion);
            command.Parameters.AddWithValue("@Status", status);
            command.Parameters.AddWithValue("@UploadTime", uploadTime);

            var queueId = (long)(await command.ExecuteScalarAsync() ?? 0L);
            _logger.LogInformation($"Inserted queue record: QueueId={queueId}, FileName={fileName}");

            return queueId;
        }

        public async Task UpdateReplicationQueueAsync(
            long queueId,
            string status,
            DateTime? startTime,
            DateTime? endTime)
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            await using var command = new SqlCommand(@"
                UPDATE FileReplicationQueue
                SET Status = @Status,
                    ReplicationStartTime = @StartTime,
                    ReplicationEndTime = @EndTime,
                    ModifiedDate = GETUTCDATE()
                WHERE QueueId = @QueueId
            ", connection);

            command.Parameters.AddWithValue("@QueueId", queueId);
            command.Parameters.AddWithValue("@Status", status);
            command.Parameters.AddWithValue("@StartTime", (object?)startTime ?? DBNull.Value);
            command.Parameters.AddWithValue("@EndTime", (object?)endTime ?? DBNull.Value);

            await command.ExecuteNonQueryAsync();
            _logger.LogInformation($"Updated queue record: QueueId={queueId}, Status={status}");
        }

        public async Task InsertReplicationAuditAsync(
            string fileName,
            long fileSize,
            string status,
            string sourceRegion,
            string targetRegion,
            DateTime uploadTime,
            DateTime replicationTime,
            int durationMs,
            string? errorMessage)
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            await using var command = new SqlCommand(@"
                INSERT INTO FileReplicationAudit 
                    (FileName, FileSize, Status, SourceRegion, TargetRegion, UploadTime, ReplicationTime, ReplicationDurationMs, ErrorMessage, CreatedDate)
                VALUES 
                    (@FileName, @FileSize, @Status, @SourceRegion, @TargetRegion, @UploadTime, @ReplicationTime, @DurationMs, @ErrorMessage, GETUTCDATE())
            ", connection);

            command.Parameters.AddWithValue("@FileName", fileName);
            command.Parameters.AddWithValue("@FileSize", fileSize);
            command.Parameters.AddWithValue("@Status", status);
            command.Parameters.AddWithValue("@SourceRegion", sourceRegion);
            command.Parameters.AddWithValue("@TargetRegion", targetRegion);
            command.Parameters.AddWithValue("@UploadTime", uploadTime);
            command.Parameters.AddWithValue("@ReplicationTime", replicationTime);
            command.Parameters.AddWithValue("@DurationMs", durationMs);
            command.Parameters.AddWithValue("@ErrorMessage", (object?)errorMessage ?? DBNull.Value);

            await command.ExecuteNonQueryAsync();
            _logger.LogInformation($"Inserted audit record: FileName={fileName}, Status={status}");
        }
    }
}