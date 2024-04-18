using System.Data.SqlClient;

namespace Proyectocsv.Model
{
    internal class conexion
    {
        public string sqlConnectionString;

        public conexion(string sqlConnectionString)
        {
            this.sqlConnectionString = sqlConnectionString;
        }

        // Métodos para abrir y cerrar conexiones para SQL Server
        public SqlConnection AbrirConexionSQLServer()
        {
            try
            {
                SqlConnection connection = new SqlConnection(sqlConnectionString);
                connection.Open();
                return connection;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error al abrir la conexión SQL Server: " + ex.Message);
                return null;
            }
        }
    }
}