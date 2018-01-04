using System;
using Dapper;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using GeoAPI.CoordinateSystems.Transformations;
using GeoAPI.Geometries;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using Newtonsoft.Json;
using ProjNet.CoordinateSystems;


namespace geojsonExporter
{
    public class Program
    {
        private const string ConnStr = "server=.;database=NiNCore_norsk;trusted_connection=yes";
        private static readonly GeoJsonWriter Writer = new GeoJsonWriter();
        private static readonly WKBReader Reader = new WKBReader();
        private static ICoordinateTransformation _trans;

        public static void Main(string[] args)
        {
            var ctFact = new ProjNet.CoordinateSystems.Transformations.CoordinateTransformationFactory();

            _trans = ctFact.CreateFromCoordinateSystems(ProjectedCoordinateSystem.WGS84_UTM(33, true), GeographicCoordinateSystem.WGS84);


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

                //if (i > 100) break;

                naturområdeJson.Geometry = ConvertToJson(geometries.First(g => g.Id == naturområdeJson.Id));

                AddBeskrivelsesVariabler(beskrivelsesVariabler, naturområdeJson);

                AddNaturområdeTyper(naturområdeTyper, naturområdeJson);

                AddRødlistekategori(rødlistekategori, kategoriSet, naturområdeJson);

                root.features.Add(naturområdeJson);

                i++;

            }

            var json = JsonConvert.SerializeObject(root).Replace("\"[[[", "[[[").Replace("]]]}\"", "]]]");

            File.WriteAllText(@"c:\tmp\naturomrader3.json", json);


        }

        private static void AddRødlistekategori(IEnumerable<Rødlistekategori> rødlistekategori, IReadOnlyCollection<KategoriSet> kategoriSet, NaturområdeJson naturområdeJson)
        {
            var rødlistekategoris = rødlistekategori as Rødlistekategori[] ?? rødlistekategori.ToArray();
            if (rødlistekategoris.All(r => r.naturområde_id != naturområdeJson.Id)) return;

            foreach (var r in rødlistekategoris.Where(r => r.naturområde_id == naturområdeJson.Id))
            {
                naturområdeJson.Properties["RØ_" + kategoriSet.First(k => k.Id == r.kategori_id).verdi] = "0";
            }
            

        }

        private static void AddNaturområdeTyper(IReadOnlyCollection<NaturområdeType> naturområdeTyper,
            NaturområdeJson naturområdeJson)
        {
            if (naturområdeTyper.All(n => n.Naturområde_id != naturområdeJson.Id)) return;
            {
                foreach (var naturområdeType in naturområdeTyper.Where(n => n.Naturområde_id == naturområdeJson.Id))
                {
                    naturområdeJson.Properties[naturområdeType.Kode] = naturområdeType.Andel.ToString();
                }
            }
        }

        private static void AddBeskrivelsesVariabler(IReadOnlyCollection<Beskrivelsesvariabel> beskrivelsesVariabler,
            NaturområdeJson naturområdeJson)
        {
            // Hvis starter med tall, splitt på underscore, ellers på bindestrek
            if (beskrivelsesVariabler.All(b => b.Naturområde_id != naturområdeJson.Id)) return;
            {
                foreach (var beskrivelsesVariabel in beskrivelsesVariabler.Where(b =>
                    b.Naturområde_id == naturområdeJson.Id).Select(b => b.Kode).ToList())
                {
                    foreach (var codePart in beskrivelsesVariabel.Split(","))
                    {
                        if (char.IsNumber(codePart.ToCharArray()[0]))
                        {
                            if (codePart.Contains("XY"))
                            {
                                naturområdeJson.Properties[codePart.Split("XY")[0]] = "XY";
                            }
                            else
                            {
                                var parts = codePart.Trim().Split('_');
                                naturområdeJson.Properties[parts[0]] = parts[1];
                            }
                        }

                        else
                        {
                            var parts = codePart.Trim().Split('-');
                            try
                            {
                                naturområdeJson.Properties[parts[0]] = parts[1];
                            }
                            catch 
                            {
                                Console.WriteLine("Beskrivelsesvariabel matcher ikke mønster: " + beskrivelsesVariabel + "for naturområde med Id " + naturområdeJson.Id);
                            }
                        }

                    }
                }
            }
        }

        private static Dictionary<string, string> ConvertToJson(Geometry geometry)
        {
            var geom = (Polygon)Reader.Read(geometry.WKB);

            TransformRing(geom.Boundary);

            foreach (var geomInteriorRing in geom.InteriorRings) TransformRing(geomInteriorRing);
            
            return new Dictionary<string, string>
            {
                {"type", geometry.GeometryType},
                {"coordinates", "[[[" + Writer.Write(geom).Split("[[[")[1]}
            };
        }

        private static void TransformRing(IGeometry ring)
        {
            for (var i = 0; i < ring.NumGeometries; i++)
            {
                TransformLinearRing(ring.GetGeometryN(i));
            }
        }

        private static void TransformLinearRing(IGeometry linearRing)
        {
            foreach (var c in linearRing.Coordinates)
            {
                c.CoordinateValue = _trans.MathTransform.Transform(c);
            }
        }

        private static List<Geometry> ReadGeometries()
        {
            using (IDbConnection db = new SqlConnection(ConnStr))
            {
                return db.Query<Geometry>("SELECT id, geometri.STGeometryType() as GeometryType, geometri.STAsBinary() as WKB FROM Naturområde").ToList();
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
        public Dictionary<string, string> Properties = new Dictionary<string, string>();
        //public crs crs = new crs();
    }

    public class Geometry
    {
        public int Id { get; set; }
        public string GeometryType { get; set; }
        public byte[] WKB { get; set; }
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