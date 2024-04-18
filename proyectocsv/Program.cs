using Proyectocsv.Model;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO.Compression;

class Program
{
    private static conexion DBModel = new conexion("Data Source=DESKTOP-F5AIHAC;Initial Catalog=Test;User ID=sa;Password=&ecurity23;");
    private static SqlConnection connection = null;
    private static string rutaDirectorio = @"D:\ProyectosC#\Archivos\csv\"; // Ruta donde están tus archivos CSV
    private static string rutaCarpetaDestino = @"D:\ProyectosC#\Archivos\csv\processed\"; // Ruta de la carpeta donde se moverán y comprimirán los archivos


    static void Main()
    {
        connection = DBModel.AbrirConexionSQLServer();

        if (connection != null)
        {
            Stopwatch totalStopwatch = Stopwatch.StartNew(); // Iniciar el cronómetro total

            ProcesarArchivosCSV(rutaDirectorio);

            totalStopwatch.Stop(); // Detener el cronómetro total
            Console.WriteLine($"Tiempo total de procesamiento: {totalStopwatch.Elapsed}");

            connection.Close();
        }
        else
        {
            Console.WriteLine("No se pudo abrir la conexión con la base de datos.");
        }
    }

    static void ProcesarArchivosCSV(string rutaDirectorio)
    {
        try
        {
            string[] archivosCSV = Directory.GetFiles(rutaDirectorio, "file*.csv");
            foreach (string archivo in archivosCSV)
            {
                Stopwatch archivoStopwatch = Stopwatch.StartNew(); // Iniciar el cronómetro para el archivo actual

                using (var lector = new StreamReader(archivo))
                {
                    // Crear un DataTable para almacenar los datos
                    DataTable dataTable = new DataTable();

                    // Configurar las columnas del DataTable basadas en la primera línea del archivo CSV
                    string[] columnas = ["campo_1", "campo_2", "campo_3"]; // Cambia esto con las columnas correctas
                    foreach (string columna in columnas)
                    {
                        dataTable.Columns.Add(columna);
                    }

                    // Leer y agregar los datos al DataTable
                    while (!lector.EndOfStream)
                    {
                        string linea = lector.ReadLine();
                        string[] valores = linea.Split(';'); // Separar por comas
                        dataTable.Rows.Add(valores);
                    }

                    // Iniciar la transacción para el bulk insert
                    SqlTransaction transaction = connection.BeginTransaction();

                    // Crear el objeto SqlBulkCopy
                    using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                    {
                        bulkCopy.DestinationTableName = "Prueba"; // Reemplaza con el nombre de tu tabla
                        bulkCopy.BatchSize = 10000; // Tamaño del lote a insertar
                        bulkCopy.BulkCopyTimeout = 600; // Tiempo de espera en segundos

                        try
                        {
                            // Realizar el bulk insert
                            bulkCopy.WriteToServer(dataTable);

                            // Commit de la transacción si todo fue exitoso
                            transaction.Commit();
                            Console.WriteLine("Bulk insert exitoso para el archivo: " + archivo);
                        }
                        catch (Exception ex)
                        {
                            // Rollback en caso de error
                            Console.WriteLine("Error durante el bulk insert para el archivo " + archivo + ": " + ex.Message);
                            transaction.Rollback();
                        }
                    }
                }

                // Pequeña pausa para permitir que el sistema libere el archivo
                System.Threading.Thread.Sleep(100);

                // Mover el archivo a otra carpeta
                string nombreArchivoSinExtension = Path.GetFileNameWithoutExtension(archivo);
                string nombreArchivoNuevo = ObtenerNombreArchivoNuevo(nombreArchivoSinExtension);
                string rutaArchivoDestino = Path.Combine(rutaCarpetaDestino, nombreArchivoNuevo + ".zip");

                // Comprimir el archivo CSV y guardarlo en un archivo ZIP
                using (FileStream archivoComprimido = File.Create(rutaArchivoDestino))
                {
                    using (var archivoZip = new ZipArchive(archivoComprimido, ZipArchiveMode.Create))
                    {
                        var entry = archivoZip.CreateEntry(Path.GetFileName(archivo), CompressionLevel.Optimal);

                        using (var writer = new StreamWriter(entry.Open()))
                        {
                            writer.Write(File.ReadAllText(archivo));
                        }
                    }
                }

                Console.WriteLine($"Archivo CSV comprimido: {rutaArchivoDestino}");

                archivoStopwatch.Stop(); // Detener el cronómetro para el archivo actual
                Console.WriteLine($"Tiempo de procesamiento para {archivo}: {archivoStopwatch.Elapsed}");
            }
        }
        catch (DirectoryNotFoundException)
        {
            Console.WriteLine($"Error: El directorio {rutaDirectorio} no existe");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
    }

    static string ObtenerNombreArchivoNuevo(string nombreArchivo)
    {
        // Obtener la letra después de "segment_"
        int indiceSegment = nombreArchivo.IndexOf("segment_") + "segment_".Length;
        string letraSegment = nombreArchivo.Substring(indiceSegment, 1);

        // Construir el nuevo nombre de archivo con el formato especificado
        string nuevoNombre = letraSegment + "_" + DateTime.Now.ToString("yyyyMMddHHmmss");

        return nuevoNombre;
    }
}
