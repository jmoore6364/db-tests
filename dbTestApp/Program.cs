using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Dynamic;
using System.Text;
using System.Threading.Tasks;
using System.Dynamic;
using System.Web.UI.WebControls.WebParts;
using Couchbase;
using Couchbase.Extensions;
using Dapper;
using Newtonsoft.Json;
using System.Web.Script.Serialization;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Driver.GridFS;
using MongoDB.Driver.Linq;
using Newtonsoft.Json.Linq;
using Simple.Data;
using Simple.Data.MongoDB;

namespace TestApp
{
    class Program
    {
        static void Main(string[] args)
        {

            //CouchbaseTest();
            //CouchbaseLoadTest_Insert();
            //CouchbaseLoadTest_Get();
            //CouchbaseLoadTest_GetBulk();
            //MongoTest();
            //Mongo_SimpleTest();
            //MongoLoadTest_Insert();
            //MongoLoadTest_GetBulk();
            //SQLLoadTest_Insert();
            //SQLLoadTest_Get();
            //ToMongoFromSql("localhost", "db", "table");
            GetDataFromMongo("collection");
            //SqlQuery("localhost", "test", "test_5034");
            //LoadMainTableInMongo("localhost");
        }

        static void CouchbaseTest()
        {
            using (var x = new CouchbaseClient("default", "")) {
                var js = new JavaScriptSerializer();
                x.ExecuteStoreJson(Enyim.Caching.Memcached.StoreMode.Set, "customer_1", new { name = "gage", location = "here, CA" });
                x.ExecuteStoreJson(Enyim.Caching.Memcached.StoreMode.Set, "customer_2", new { name = "jason", location = "somwhere, CA" });
                x.ExecuteStoreJson(Enyim.Caching.Memcached.StoreMode.Set, "customer_3", new { name = "roger", location = "Houston, TX" });
                dynamic customer1 = x.ExecuteGetJson<ExpandoObject>("customer_1").Value;
                Console.Write(customer1.name);
            }

        }

        static void CouchbaseLoadTest_Insert()
        {
            Console.WriteLine(DateTime.Now);
          
            using (var x = new CouchbaseClient("default", ""))
            {
                for (var i = 0; i < 500000; i++)
                {
                    x.ExecuteStoreJson(Enyim.Caching.Memcached.StoreMode.Set, "customer_" + i, new { 
                        name = "customer" + i,
                        data1 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                        data2 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                        data3 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                        data4 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                        //data5 = "sjdhsdfkghiuwerytuierytdjkfhgkdjsjdhsdfkghiuwerytuierytdjkfhgkdjfhgsdgfsdlfjseityerukytdfhgsdgfsdlfjseityerukytd",
                        //data6 = "sjdhsdfkghiuwerytuierytdjkfhgkdjfhgsdgfsdlfjseityerukytd",
                        //data7 = "sjdhsdfkghiuwerytuierytdjkfhgkdjfhgsdgfsdlfjseityerukytd",
                        //data8 = "sjdhsdfkghiuwerytuierytdjkfhgkdjfhsjdhsdfkghiuwerytuierytdjkfhgkdjfhgsdgfsdlfjseityerukytdgsdgfsdlfjseityerukytd",
                        //data9 = "sjdhsdfkghiuwerytuierytdjkfhgkdjfhgsdgfsdlfjseityerukytd",
                        //data10 = "sjdhsdfkghiuwerytuierytdjkfhgkdjfhgsdgfsdlfjseityerukytd",
                        //data11 = "sjdhsdfkghiuwerytuierytdjkfhgkdjfhgsjdhsdfkghiuwerytuierytdjkfhgkdjfhgsdgfsdlfjseityerukytdsdgfsdlfjseityerukytd",
                        //data12 = "sjdhsdfkghiuwerytuierytdjkfhgkdjfhgsdgfsdlfjseityerukytd",
                        //data13 = "sjdhsdfkghiuwerytuierytdjkfhgkdjfhgsdgfsdlfjseityerukytd",
                        //data14 = "sjdhsdfkghiuwerytuierytdjkfhgkdjfsjdhsdfkghiuwerytuierytdjkfhgkdjfhgsdgfsdlfjseityerukytdhgsdgfsdlfjseityerukytd",
                        //data15 = "sjdhsdfkghiuwerytuierytdjkfhgkdjfhgsdgfsdlfjseityerukytd",
                        //data16 = "sjdhsdfkghiuwerytuierytdjkfhgkdjfhgssjdhsdfkghiuwerytuierytdjkfhgkdjfhgsdgfsdlfjseityerukytddgfsdlfjseityerukytd",
                        //data17 = "sjdhsdfkghiuwerytuierytdjsjdhsdfkghiuwerytuierytdjkfhgkdjfhgsdgfsdlfjseityerukytdkfhgkdjfhgsdgfsdlfjseityerukytd",
                        //data18 = "sjdhsdfkghiuwerytuierytdjkfhgkdjfhgsdgfsdlfjseityerukytd",
                        //data19 = "sjdhsdfkghiuwerytuierytdjkfhgkdjfhgsdgfsdlfjseityerukytd",
                        //data20 = "sjdhsdfkghiuwerytuierytdjkfhgkdjfsjdhsdfkghiuwerytuierytdjkfhgkdjfhgsdgfsdlfjseityerukytdhgsdgfsdlfjseityerukytd",
                        //data21 = "sjdhsdfkghiuwerytuierytdjkfhgkdjfhgsdgfsdlfjseityerukytd",
                        //data22 = "sjdhsdfkghiuwerytuierytdjkfhgkdjfhgssjdhsdfkghiuwerytuierytdjkfhgkdjfhgsdgfsdlfjseityerukytddgfsdlfjseityerukytd",
                        //data23 = "sjdhsdfkghiuwerytuierytdjsjdhsdfkghiuwerytuierytdjkfhgkdjfhgsdgfsdlfjseityerukytdkfhgkdjfhgsdgfsdlfjseityerukytd",
                        //data24 = new int[] {1,2,3,4,5,}
                    });
                }
                
            }
            Console.WriteLine(DateTime.Now);
            Console.ReadKey();
        }

        static void CouchbaseLoadTest_Get()
        {
            var customers = new List<dynamic>();
            Console.WriteLine(DateTime.Now);
            using (var x = new CouchbaseClient("default", ""))
            {
                for (var i = 0; i < 100000; i++)
                {
                    customers.Add(x.ExecuteGetJson<ExpandoObject>("customer_" + i).Value);
                }

            }
            Console.WriteLine(DateTime.Now);
            Console.ReadKey();
        }

        static void CouchbaseLoadTest_GetBulk()
        {
            var keys = new List<string>();
            for (var i = 0; i < 500000; i++)
                keys.Add("customer_" + i);
            Console.WriteLine(DateTime.Now);
            using (var x = new CouchbaseClient("default", ""))
            {
                var customers = x.Get(keys);
            }
            Console.WriteLine(DateTime.Now);
            if (Console.ReadKey().Key == ConsoleKey.R)
                CouchbaseLoadTest_GetBulk();
        }

        class customer
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Location { get; set; }
            public string Data1 { get; set; }
            public string Data2 { get; set; }
            public string Data3 { get; set; }
            public string Data4 { get; set; }
            public string Data5 { get; set; }
            public string Data6 { get; set; }
            public string Data7 { get; set; }
            public string Data8 { get; set; }
            public string Data9 { get; set; }
            public string Data10 { get; set; }
            public string Data11 { get; set; }
            public string Data12 { get; set; }
            public string Data13 { get; set; }
            public string Data14 { get; set; }
            public string Data15 { get; set; }
            public string Data16 { get; set; }
            public string Data17 { get; set; }
            public string Data18 { get; set; }
            public string Data19 { get; set; }
            public string Data20 { get; set; }
        }

        static void MongoTest()
        {
            var connectionString = "mongodb://localhost";
            var client = new MongoClient(connectionString);
            var server = client.GetServer();
            var database = server.GetDatabase("local");
            var collection = database.GetCollection<ExpandoObject>("test");
            var customer = new customer
            {
                Id = "customer1",
                Name = "jack",
                Location = "somewhere, CA",
                Data1 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                Data2 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                Data3 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                Data4 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                Data5 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                Data6 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                Data7 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                Data8 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
            };
            collection.Insert(customer);
        }

        static void MongoLoadTest_Insert()
        {
            var connectionString = "mongodb://localhost";
            var client = new MongoClient(connectionString);
            var server = client.GetServer();
            var database = server.GetDatabase("local");
            var collection = database.GetCollection<customer>("test");
            Console.WriteLine(DateTime.Now);
            for (var i = 0; i < 500000; i++)
            {
                collection.Save(new customer
                {
                    Id = "customer" + i,
                    Data1 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                    Data2 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                    Data3 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                    Data4 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                    Data5 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                    Data6 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                    Data7 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                    Data8 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                    Data9 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                    Data10 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                    Data11 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                    Data12 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                    Data13 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                    Data14 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                    Data15= "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                    Data16 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                    Data17 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                    Data18 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                    Data19 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                    Data20 = "jkhdrt8923758ehrgsd;nblt'oktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E",
                });
            }
            Console.WriteLine(DateTime.Now);
            Console.ReadKey();
        }

        static void MongoLoadTest_GetBulk()
        {
            var connectionString = "mongodb://localhost";
            var client = new MongoClient(connectionString);
            var server = client.GetServer();
            var database = server.GetDatabase("local");
            var collection = database.GetCollection<customer>("test");
            
            Console.WriteLine(DateTime.Now);
            var customers = collection.AsQueryable<ExpandoObject>().ToList();
            Console.WriteLine(DateTime.Now);
            Console.ReadKey();
        }

        static void MongoLoadTest_Query()
        {
            var connectionString = "mongodb://localhost";
            var client = new MongoClient(connectionString);
            var server = client.GetServer();
            var database = server.GetDatabase("local");
            var collection = database.GetCollection<customer>("test");

            Console.WriteLine(DateTime.Now);
            var customers = collection.AsQueryable<customer>().Where(q => q.Id == "Customer23000");
            Console.WriteLine(DateTime.Now);
            
            Console.ReadKey();
        }
        
        static void ToMongoFromSql(string server, string db, string table)
        {
            
            IEnumerable<dynamic> data;
            using (var con = new SqlConnection(String.Format("server={0};database={1};Trusted_Connection=true;", server, db)))
                data = con.Query(String.Format("select * from {0}", table));

            var database = new MongoClient("mongodb://localhost").GetServer().GetDatabase("local");
            MongoCollection<BsonDocument> collection = null;
            if (database.CollectionExists(table))
                database.DropCollection(table);
            database.CreateCollection(table);
            collection = database.GetCollection(table);
            var d = DateTime.Now;
            foreach (var row in data)
                collection.Save(new BsonDocument(row));
            Console.WriteLine(DateTime.Now.Subtract(d).TotalSeconds);
            Console.ReadKey();
        }

        static void MongoSave(string collectionName, IEnumerable<dynamic> data)
        {
            var database = new MongoClient("mongodb://localhost").GetServer().GetDatabase("local");
            if (!database.CollectionExists(collectionName))
                database.CreateCollection(collectionName);
            var collection = database.GetCollection(collectionName);
            foreach (var row in data)
                collection.Save(new BsonDocument(row));
        }

        static void LoadMainTableInMongo(string server)
        {
            var masters = SqlQuery(server, "db", "table");
            foreach (dynamic master in masters)
            {
                var table = "table_" + master.ID;
                IEnumerable<dynamic> data = SqlQuery(server, "sqlDB", table);
                MongoSave("collection", data);
                data = null;
            }
        }

        static IEnumerable<dynamic> SqlQuery(string server, string db, string table)
        {
            IEnumerable<dynamic> data;
            var d = DateTime.Now;
            using (var con = new SqlConnection(String.Format("server={0};database={1};Trusted_Connection=true;", server, db)))
                data = con.Query(String.Format("select * from {0}", table));
            Console.WriteLine(DateTime.Now.Subtract(d).TotalSeconds);
            return data;
        }

        static void GetDataFromMongo(string collectionName)
        {
            var d = DateTime.Now;
            var collection = (new MongoClient("")
                .GetServer()
                .GetDatabase("jason2"))
                .GetCollection(collectionName)
                .AsQueryable<BsonDocument>().Where(x => x["_id"] == "customer100").ToList();
            Console.WriteLine(collection.Count());
            Console.WriteLine(DateTime.Now.Subtract(d).TotalSeconds);
            Console.ReadKey();
        }

        static void Mongo_SimpleTest()
        {
            var connectionString = "mongodb://localhost/local";
            dynamic db = Database.Opener.OpenMongo(connectionString);
            db.test.Insert(Id: "customer1", Name: "test", Location: "Somewhere, CA" );
        }

        public static bool IsSettingsExist(dynamic settings, string name)
        {
            return settings.GetType().GetProperty(name) != null;
        }

        static void SQLLoadTest_Insert()
        {
            using (var con = new SqlConnection("server=localhost;database=test;Trusted_Connection=true"))
            {
                con.Open();
                using (var cmd = new SqlCommand("", con))
                {
                    Console.WriteLine(DateTime.Now);
                    for (var i = 0; i < 500001; i++)
                    {
                        cmd.CommandText = @"
                            insert into customer (name, data1, data2, data3, data4)
	                            values (
		                            'customer" + i + @"',
		                            'jkhdrt8923758ehrgsd;nbltoktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E',
                                    'jkhdrt8923758ehrgsd;nbltoktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E',
                                    'jkhdrt8923758ehrgsd;nbltoktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E',
                                    'jkhdrt8923758ehrgsd;nbltoktqr9ie[29384Q9W[EURQ8W4TUPQ98734THFQWENRPV987WSRYGWP98E'
	                            )
                            ";
                        cmd.ExecuteNonQuery();
                    }
                    Console.WriteLine(DateTime.Now);
                    Console.ReadKey();
                }
            }

        }

        static void SQLLoadTest_Get()
        {
            var customers = new List<dynamic>();
            using (var con = new SqlConnection("server=localhost;database=test;Trusted_Connection=true"))
            {
                con.Open();
                Console.WriteLine(DateTime.Now);
                //con.Query("select * from customer");
                using (var cmd = new SqlCommand("", con))
                {

                    cmd.CommandText = "select * from customer";
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            customers.Add(new
                            {
                                name = reader["name"].ToString(),
                                data1 = reader["data1"].ToString(),
                                data2 = reader["data2"].ToString(),
                                data3 = reader["data3"].ToString(),
                                data4 = reader["data4"].ToString(),
                                data5 = reader["data5"].ToString(),
                                data6 = reader["data6"].ToString(),
                                data7 = reader["data7"].ToString(),
                                data8 = reader["data8"].ToString(),
                                data9 = reader["data9"].ToString(),
                                data10 = reader["data10"].ToString(),
                                data11 = reader["data11"].ToString(),
                                data12 = reader["data12"].ToString(),
                                data13 = reader["data13"].ToString(),
                                data14 = reader["data14"].ToString(),
                                data15 = reader["data15"].ToString(),
                                data16 = reader["data16"].ToString(),
                                data17 = reader["data17"].ToString(),
                                data18 = reader["data18"].ToString(),
                                data19 = reader["data19"].ToString(),
                                data20 = reader["data20"].ToString(),
                            });
                        }
                    }

                }
            }
            Console.WriteLine(DateTime.Now);
            Console.ReadKey();

        }

        
    }

}
