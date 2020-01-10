using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Threading.Tasks;
using System.Linq;

namespace cosmosdb.sqlapi.pagination
{
    class Program
    {
        private static DocumentClient client = new DocumentClient(new Uri("https://haritest.documents.azure.com:443/"), "");

        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            var database = await client.CreateDatabaseIfNotExistsAsync(new Microsoft.Azure.Documents.Database() { Id = "catalogue" });
            DocumentCollection collectionDefinition = new DocumentCollection();
            collectionDefinition.Id = "products";
            collectionDefinition.IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) { Precision = -1 });
            collectionDefinition.PartitionKey.Paths.Add("/Id");

            var documentCollection = await GetOrCreateCollectionAsync("catalogue", "products");
            Uri collectionUri = UriFactory.CreateDocumentCollectionUri("catalogue", "products");
            await CreateDocuments(collectionUri);

             QueryDocumentsAsPages(collectionUri);
        }

        private static void QueryDocumentsAsPages(Uri collectionUri)
        {
            var documentCountQuery = client.CreateDocumentQuery<dynamic>(collectionUri, new SqlQuerySpec()
            {
                QueryText = "SELECT VALUE COUNT(1) FROM products"
            });
            long documentCount = 0;
            foreach(var value in documentCountQuery)
            {
                Console.WriteLine($"Total documents - {value}");
                documentCount = value;
            }
            


            var pageSize = 10;
            var numberOfPages = documentCount / pageSize;
            var offset = 0;
            for (int i = 0; i < numberOfPages; i++)
            {
                var query = client.CreateDocumentQuery<Product>(collectionUri, new SqlQuerySpec()
                {
                    QueryText = "SELECT * FROM products OFFSET @offset LIMIT @limit",
                    Parameters = new SqlParameterCollection()
                        {
                            new SqlParameter("@offset", offset),
                            new SqlParameter("@limit", pageSize)
                        }
                }, new FeedOptions() { EnableCrossPartitionQuery = true});

                var products = query.ToList();
                Console.WriteLine($"First product id - {products.First().ProductId} - Last product id - {products.Last().ProductId}");
                offset = offset + pageSize;
            }

            //var families = query.ToList();
        }

        private static async Task<DocumentCollection> GetOrCreateCollectionAsync(string databaseId, string collectionId)
        {
            DocumentCollection collectionDefinition = new DocumentCollection();
            collectionDefinition.Id = collectionId;
            collectionDefinition.IndexingPolicy = new IndexingPolicy(new RangeIndex(DataType.String) { Precision = -1 });
            collectionDefinition.PartitionKey.Paths.Add("/ProductId");

            return await client.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri(databaseId),
                collectionDefinition,
                new RequestOptions { OfferThroughput = 400 });
        }

        private static async Task CreateDocuments(Uri collectionUri)
        {
            for (int i = 1;  i <= 500; i++)
            {
                Product product = new Product()
                {
                    ProductId = i,
                    Name = $"Product Name {i}"
                };
                await client.UpsertDocumentAsync(collectionUri, product);
            }
        }
    }
}
