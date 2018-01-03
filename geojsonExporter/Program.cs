using Dapper;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Newtonsoft.Json;

namespace geojsonExporter
{
    public class Program
    {
        private const string ConnStr = "server=.;database=NiNCore_norsk;trusted_connection=yes";

        public static void Main(string[] args)
        {
            var naturområdeTyper = ReadAll<NaturområdeType>("NaturområdeType");
            var beskrivelsesVariabler = ReadAll<Beskrivelsesvariabel>("Beskrivelsesvariabel");
            var naturområder = ReadAll<NaturområdeJson>("Naturområde");
            var rødlistekategori = ReadAll<Rødlistekategori>("Rødlistekategori");
            var kategoriSet = ReadAll<KategoriSet>("KategoriSet");

            var geometries = ReadGeometries();

            var root = new root();

            var i = 0;
            foreach (var naturområdeJson in naturområder)
            {
                if (naturområdeTyper.All(n => n.Naturområde_id != naturområdeJson.Id) &&
                    beskrivelsesVariabler.All(b => b.Naturområde_id != naturområdeJson.Id)) continue;

                if (i > 100) break;

                naturområdeJson.Geometry = ConvertToJson(geometries.First(g => g.Id == naturområdeJson.Id));

                AddBeskrivelsesVariabler(beskrivelsesVariabler, naturområdeJson);

                AddNaturområdeTyper(naturområdeTyper, naturområdeJson);

                AddRødlistekategori(rødlistekategori, kategoriSet, naturområdeJson);

                root.features.Add(naturområdeJson);

                i++;

            }

            var json = JsonConvert.SerializeObject(root).Replace("\"[[[", "[[[").Replace("]]]\"", "]]]");


        }

        private static void AddRødlistekategori(IEnumerable<Rødlistekategori> rødlistekategori, IReadOnlyCollection<KategoriSet> kategoriSet, NaturområdeJson naturområdeJson)
        {
            var rødlistekategoris = rødlistekategori as Rødlistekategori[] ?? rødlistekategori.ToArray();
            if (rødlistekategoris.All(r => r.naturområde_id != naturområdeJson.Id)) return;

            foreach (var r in rødlistekategoris.Where(r => r.naturområde_id == naturområdeJson.Id))
            {
                naturområdeJson.Properties["RØ_" + kategoriSet.First(k => k.Id == r.kategori_id).verdi] = 0;
            }
            

        }

        private static void AddNaturområdeTyper(IReadOnlyCollection<NaturområdeType> naturområdeTyper,
            NaturområdeJson naturområdeJson)
        {
            if (naturområdeTyper.All(n => n.Naturområde_id != naturområdeJson.Id)) return;
            {
                foreach (var naturområdeType in naturområdeTyper.Where(n => n.Naturområde_id == naturområdeJson.Id))
                {
                    naturområdeJson.Properties[naturområdeType.Kode] = naturområdeType.Andel;
                }
            }
        }

        private static void AddBeskrivelsesVariabler(IReadOnlyCollection<Beskrivelsesvariabel> beskrivelsesVariabler,
            NaturområdeJson naturområdeJson)
        {
            if (beskrivelsesVariabler.All(b => b.Naturområde_id != naturområdeJson.Id)) return;
            {
                foreach (var beskrivelsesVariabel in beskrivelsesVariabler.Where(b =>
                    b.Naturområde_id == naturområdeJson.Id).Select(b => b.Kode).ToList())
                {
                    if (beskrivelsesVariabel.Contains(","))
                    {
                        foreach (var codePart in beskrivelsesVariabel.Split(","))
                        {
                            naturområdeJson.Properties[codePart.Trim()] = 0;
                        }
                    }
                    else naturområdeJson.Properties[beskrivelsesVariabel] = 0;
                }
            }
        }

        private static Dictionary<string, string> ConvertToJson(Geometry geometry)
        {
            return new Dictionary<string, string>
            {
                {"type", geometry.GeometryType},
                {"coordinates", geometry.Coordinates}
            };
        }

        private static List<Geometry> ReadGeometries()
        {
            using (IDbConnection db = new SqlConnection(ConnStr))
            {
                //return db.Query<Geometry>("SELECT id, geometri.STGeometryType() as GeometryType, geometri.ToString() as WKT FROM Naturområde").ToList();

                return db.Query<Geometry>(
                        @"SELECT id, geometri.STGeometryType() as GeometryType,(CASE geometri.STGeometryType() WHEN 'POINT' THEN REPLACE(REPLACE(REPLACE(REPLACE(geometri.ToString(), UPPER(geometri.STGeometryType()) + ' ', ''), '(', '['), ')', ']'), ' ', ',')" +
                        " ELSE '[' + REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(geometri.ToString(), UPPER(geometri.STGeometryType()) + ' ', ''), '(', '['), ')', ']'), '], ', ']],['),', ','],['),' ',', ') + ']' END) as Coordinates FROM Naturområde")
                    .ToList();
            }
        }

        public static List<T> ReadAll<T>(string tableName)
        {
            using (IDbConnection db = new SqlConnection(ConnStr))
            {
                return db.Query<T>("SELECT * FROM " + tableName).ToList();
            }
        }
    }

    public class KategoriSet
    {
        public int Id { get; set; }
        public string verdi { get; set; }
    }

    public class Rødlistekategori
    {
        public int kategori_id { get; set; }
        public int naturområde_id { get; set; }

    }

    public class Beskrivelsesvariabel
    {
        public int Id { get; set; }
        public string Kode { get; set; }
        public int Naturområde_id { get; set; }
    }

    public class NaturområdeType
    {
        public int Id { get; set; }
        public string Kode { get; set; }
        public int Andel { get; set; }
        public int Naturområde_id { get; set; }
    }

    public class NaturområdeJson
    {
        public string type = "Feature";
        public Dictionary<string, string> Geometry = new Dictionary<string, string>();
        public int Id { get; set; }
        public Dictionary<string, int> Properties = new Dictionary<string, int>();
        public crs crs = new crs();
    }

    public class Geometry
    {
        public int Id { get; set; }
        public string GeometryType { get; set; }
        public string Coordinates { get; set; }
    }

    public class crs
    {
        public string type = "name";

        public Dictionary<string, string> properties = new Dictionary<string, string>
        {
            {"name", "EPSG:32633"}
        };
    }

    public class root
    {
        public string type = "FeatureCollection";
        public List<NaturområdeJson> features = new List<NaturområdeJson>();
    }

}