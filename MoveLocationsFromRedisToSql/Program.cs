using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using System.Data;
using System.Data.SqlClient;
using static Dapper.SqlMapper;
using Newtonsoft.Json;

namespace MoveLocationsFromRedisToSql
{
	public sealed class Settings
	{
		public string RedisConnectionString { get; set; }
		public string SqlConnectionString { get; set; }
	}

	public class UserLocation
	{
		public Guid UserID { get; set; }
		public double Latitude { get; set; }
		public double Longitude { get; set; }
		public bool IsStale { get; set; }
		public DateTimeOffset Timestamp { get; set; }
	}

	internal class Program
	{
		static void Main(string[] args)
		{
			IConfiguration config = new ConfigurationBuilder()
				.AddJsonFile("appsettings.json")
				.Build();
			Settings settings = config.GetRequiredSection("Settings").Get<Settings>();

			var connection = ConnectionMultiplexer.Connect(settings.RedisConnectionString);
			var servers = connection.GetServers();
			var db = connection.GetDatabase();
			var userLocations = new List<UserLocation>();
			foreach (var server in servers)
			{
				var keys = server.Keys(pattern: "lba:*");
				foreach(var key in keys)
				{
					var userKeys = db.HashKeys(key);
					foreach (var userKey in userKeys)
					{
						var uv = db.HashGet(key, userKey);
						var userLocation = JsonConvert.DeserializeObject<UserLocation>(uv);
						userLocation.UserID = Guid.Parse(userKey);
						userLocations.Add(userLocation);
					}
				}
			}

			using (IDbConnection dbConnection = new SqlConnection(settings.SqlConnectionString))
			{
				dbConnection.Open();
				userLocations.ForEach(ul =>
				{
					dbConnection.Execute("INSERT INTO UserLocation VALUES (NEWID(), @userID, @latitude, @longitude, geography::Point(@latitude, @longitude, 4326), GETUTCDATE(), 0)", ul);
				});
			}
		}
	}
}