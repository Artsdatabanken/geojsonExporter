using System;
using Dapper;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
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
            var rødlistekategori = ReadAll<Rødlistekategori>("Rødlistekategori");
            var kategoriSet = ReadAll<KategoriSet>("KategoriSet");
            var rødlisteVurderingsenhetSet = ReadAll<RødlisteVurderingsenhetSet>("RødlisteVurderingsenhetSet");

            var geometries = ReadGeometries();

            var root = new root();

            var i = 0;

            foreach (var naturområde in geometries)
            {
                i++;

                if (naturområdeTyper.All(n => n.Naturområde_id != naturområde.Id) &&
                    beskrivelsesVariabler.All(b => b.Naturområde_id != naturområde.Id)) continue;

                //if (i > 100) break;

                try
                {
                    var flexible = GenerateFlexible(naturområde);

                    AddBeskrivelsesVariabler(beskrivelsesVariabler, flexible);

                    AddNaturområdeTyper(naturområdeTyper, flexible);

                    AddRødlistekategori(rødlisteVurderingsenhetSet, rødlistekategori, kategoriSet, flexible);

                    root.features.Add(flexible);
                }
                catch (Exception e)
                {
                    Console.Write("ERROR: Something went wrong with Id " + naturområde.Id);
                    Console.WriteLine();
                    continue;
                }

                

                Console.Write("\r{0}%   ", (1.0*i/ geometries.Count)*100);
            }

            var json = JsonConvert.SerializeObject(root);

            File.WriteAllText(@"c:\tmp\naturomrader4.json", json);


        }

        private static dynamic GenerateFlexible(Naturområde naturområde)
        {
            dynamic flexible = new ExpandoObject();

            flexible.type = "Feature";

            flexible.properties = new Dictionary<string, string>();

            flexible.geometry = ConvertToJson(naturområde.WKB);

            flexible.id = naturområde.Id;

            return flexible;
        }

        private static void AddRødlistekategori(List<RødlisteVurderingsenhetSet> rødlisteVurderingsenhetSet, List<Rødlistekategori> rødlistekategori, IReadOnlyCollection<KategoriSet> kategoriSet, dynamic naturområdeJson)
        {
            if (rødlistekategori.All(r => r.naturområde_id != naturområdeJson.id)) return;

            foreach (var r in rødlistekategori.Where(r => r.naturområde_id == naturområdeJson.id))
            {
                naturområdeJson.properties["RKAT_" + kategoriSet.First(k => k.Id == r.kategori_id).verdi] = rødlisteVurderingsenhetSet.First( rv => rv.id == r.rødlistevurderingsenhet_id).verdi;
            }
            

        }

        private static void AddNaturområdeTyper(IReadOnlyCollection<NaturområdeType> naturområdeTyper,
            dynamic naturområdeJson)
        {
            if (naturområdeTyper.All(n => n.Naturområde_id != naturområdeJson.id)) return;
            {
                foreach (var naturområdeType in naturområdeTyper.Where(n => n.Naturområde_id == naturområdeJson.id))
                {
                    naturområdeJson.properties[naturområdeType.Kode] = naturområdeType.Andel.ToString();
                }
            }
        }

        private static void AddBeskrivelsesVariabler(IReadOnlyCollection<Beskrivelsesvariabel> beskrivelsesVariabler,
            dynamic naturområdeJson)
        {
            // Hvis starter med tall, splitt på underscore, ellers på bindestrek
            if (beskrivelsesVariabler.All(b => b.Naturområde_id != naturområdeJson.id)) return;
            {
                foreach (var beskrivelsesVariabel in beskrivelsesVariabler.Where(b =>
                    b.Naturområde_id == naturområdeJson.id).Select(b => b.Kode).ToList())
                {
                    foreach (var codePart in beskrivelsesVariabel.Split(","))
                    {
                        if (char.IsNumber(codePart.ToCharArray()[0]))
                        {
                            if (codePart.Contains("_"))
                            {
                                var parts = codePart.Trim().Split('_');
                                naturområdeJson.properties[parts[0]] = parts[1];
                            }
                            else
                            {
                                naturområdeJson.properties[codePart] = "null";
                            }
                        }

                        else
                        {
                            var parts = codePart.Trim().Split('-');
                            if (parts.Length > 1) naturområdeJson.properties[parts[0]] = parts[1];
                            else naturområdeJson.properties[parts[0]] = "null";
                        }

                    }
                }
            }
        }

        private static object ConvertToJson(byte[] geometry)
        {
            var geom = Reader.Read(geometry);
            
            if (!geom.IsValid) throw new Exception();

            return JsonConvert.DeserializeObject(Writer.Write(geom));

            for (var i = 0; i < geom.NumGeometries; i++)
            {
                TransformRing(((Polygon)geom.GetGeometryN(i)).Boundary);
            }

            if (geom.IsValid)
                return JsonConvert.DeserializeObject(Writer.Write(geom));

            

            throw new Exception();
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
            for (var index = 0; index < linearRing.Coordinates.Length; index++)
            {
                linearRing.Coordinates[index].CoordinateValue = _trans.MathTransform.Transform(linearRing.Coordinates[index].CoordinateValue);
            }
        }

        private static List<Naturområde> ReadGeometries()
        {
            using (IDbConnection db = new SqlConnection(ConnStr))
            {
                return db.Query<Naturområde>("SELECT id, geometri.STGeometryType() as GeometryType, geometri.STAsBinary() as WKB FROM Naturområde").ToList();
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

    public class RødlisteVurderingsenhetSet
    {
        public int id { get; set; }
        public string verdi { get; set; }
    }

    public class Crs
    {
        public string type = "name";
        public Dictionary<string, string> properties = new Dictionary<string, string>
        {
            { "name", "urn:ogc:def:crs:OGC:1.3:CRS84" }
        };
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
        public int rødlistevurderingsenhet_id { get; set; }

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

    public class Naturområde
    {
        public int Id { get; set; }
        public string GeometryType { get; set; }
        public byte[] WKB { get; set; }
    }

    public class root
    {
        public string type = "FeatureCollection";
        public Crs crs = new Crs();
        public List<ExpandoObject> features = new List<ExpandoObject>();
    }
}