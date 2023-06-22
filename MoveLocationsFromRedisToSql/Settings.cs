namespace MoveLocationsFromRedisToSql;

public sealed class Settings
{
	public string RedisConnectionString { get; set; }
	public string SqlConnectionString { get; set; }
}