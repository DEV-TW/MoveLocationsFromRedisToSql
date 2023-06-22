namespace MoveLocationsFromRedisToSql;

public class UserLocation
{
	public Guid UserID { get; set; }
	public double Latitude { get; set; }
	public double Longitude { get; set; }
	public bool IsStale { get; set; }
	public DateTimeOffset Timestamp { get; set; }
}