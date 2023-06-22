using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using System.Data;
using System.Data.SqlClient;
using static Dapper.SqlMapper;
using Newtonsoft.Json;

namespace MoveLocationsFromRedisToSql
{
	internal class Program
	{
		static void Main(string[] args)
		{
			IConfiguration config = new ConfigurationBuilder()
				.AddJsonFile("appsettings.json")
				.Build();
			var settings = config.GetRequiredSection("Settings").Get<Settings>();

			if (settings != null)
			{
				var connection = ConnectionMultiplexer.Connect(settings.RedisConnectionString);
				var servers = connection.GetServers();
				var db = connection.GetDatabase();
				var userLocations = new List<UserLocation>();

				Console.WriteLine("Pulling locations out of redis");
				foreach (var server in servers)
				{
					var keys = server.Keys(pattern: "lba:*");
					foreach (var key in keys)
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

				Console.WriteLine("Writing locations to Database");
				using (IDbConnection dbConnection = new SqlConnection(settings.SqlConnectionString))
				{
					dbConnection.Open();
					using (var transaction = dbConnection.BeginTransaction())
					{
						userLocations.ForEach(ul =>
						{
							dbConnection.Execute("INSERT INTO UserLocation VALUES (NEWID(), @userID, @latitude, @longitude, geography::Point(@latitude, @longitude, 4326), GETUTCDATE(), 0)", ul, transaction: transaction);
						});
						transaction.Commit();
					}
				}

				Console.WriteLine("All done.");
			}

			
		}
	}
}