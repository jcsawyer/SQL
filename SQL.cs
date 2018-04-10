using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using System.Web.Configuration;
using System.Collections;

namespace JCSBlog.Core
{
    public class SQL
    {
        private static string ConnectionString = WebConfigurationManager.AppSettings["AppConnectionString"].ToString();
        private static SqlConnection conn;

        /// <summary>
        /// Check whether data exists in database
        /// </summary>
        /// <param name="Table">Table name</param>
        /// <param name="ColumnValues">Column names and values</param>
        /// <returns>Boolean if it exists or not</returns>
        public static bool Exists(string Table, Hashtable ColumnValues)
        {
            string parameters = "";
            int num = 0;
            SqlCommand comm = new SqlCommand();
            try
            {
                using (conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();
                    foreach (string str in ColumnValues)
                    {
                        num++;
                        parameters = parameters + "[" + str + "] = @" + str;
                        if (num != ColumnValues.Keys.Count)
                            parameters = parameters + " AND ";
                        SqlParameter parameter = new SqlParameter();
                        parameter.ParameterName = "@" + str;
                        parameter.Value = ColumnValues[str];
                        comm.Parameters.Add(parameter);
                    }
                    comm.CommandText = "SELECT COUNT(*) FROM " + Table + " WHERE " + parameters;
                    comm.Connection = conn;

                    int result = Convert.ToInt32(comm.ExecuteScalar().ToString());
                    if (result >= 1)
                        return true;
                    else
                        return false;
                }
            }
            catch (Exception ex)
            {
                Utilities.LogError(ex);
                return false;
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                    conn.Dispose();
                }
                comm.Dispose();
            }
        }

        /// <summary>
        /// Gets a count of rows
        /// </summary>
        /// <param name="Table">Table name</param>
        /// <param name="WhereClause">Where clause</param>
        /// <returns>Number of rows</returns>
        public static int Count(string Table, string WhereClause)
        {
            int result = 0;
            SqlCommand comm = new SqlCommand();
            try
            {
                using (conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();
                    comm.CommandText = "SELECT COUNT(*) FROM " + Table + " WHERE " + WhereClause;
                    comm.Connection = conn;
                    result = Convert.ToInt32(comm.ExecuteScalar().ToString());
                }
            }
            catch (Exception ex)
            {
                Utilities.LogError(ex);
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                    conn.Dispose();
                }
                comm.Dispose();
            }
            return result;
        }

        /// <summary>
        /// Executes a simple SQL query and returns a value
        /// </summary>
        /// <param name="Command">SQL query</param>
        /// <returns>The first cell of first row returned from query</returns>
        public static string ExecuteSQL(string Command)
        {
            string result = "";
            SqlCommand comm = new SqlCommand();
            try
            {
                using (conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();
                    comm.CommandText = Command;
                    comm.Connection = conn;
                    result = comm.ExecuteScalar().ToString();
                }
            }
            catch (Exception ex)
            {
                Utilities.LogError(ex);
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                    conn.Dispose();
                }
                comm.Dispose();
            }
            return result;
        }

        /// <summary>
        /// Executes a command and returns a filled DataSet
        /// </summary>
        /// <param name="Command">SQL query</param>
        /// <returns>Filled DataSet</returns>
        public static DataSet ExecuteDataSet(string Command)
        {
            DataSet ds = new DataSet();
            SqlDataAdapter da = new SqlDataAdapter();
            SqlCommand comm = new SqlCommand();
            try
            {
                using (conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();
                    comm.CommandText = Command;
                    comm.Connection = conn;
                    da.SelectCommand = comm;
                    da.Fill(ds);
                }
            }
            catch (Exception ex)
            {
                Utilities.LogError(ex);
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                    conn.Dispose();
                }
                da.Dispose();
                comm.Dispose();
            }
            return ds;
        }

        /// <summary>
        /// Deletes data from database
        /// </summary>
        /// <param name="Table">Table name</param>
        /// <param name="WhereClause">Where clause</param>
        public static void DeleteData(string Table, string WhereClause)
        {
            SqlCommand comm = new SqlCommand();
            try
            {
                using (conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();
                    comm.CommandText = "DELETE FROM " + Table + " WHERE " + WhereClause;
                    comm.Connection = conn;
                    comm.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Utilities.LogError(ex);
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                    conn.Dispose();
                }
                comm.Dispose();
            }
        }

        /// <summary>
        /// Execute a query and return a filled hashtable
        /// </summary>
        /// <param name="Table">Table name</param>
        /// <param name="Columns">Columns</param>
        /// <param name="Where">Where clause</param>
        /// <param name="OrderBy">Order by clause</param>
        /// <returns>Hashtable containing DataSet, DataTable and DataCount</returns>
        public static Hashtable SimpleQuery(string Table, string Columns, string WhereClause, string OrderBy)
        {
            Hashtable result = new Hashtable();
            DataSet ds = new DataSet();
            DataTable dt = new DataTable();
            SqlDataAdapter da = new SqlDataAdapter();
            SqlCommand comm = new SqlCommand();
            try
            {
                using (conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();
                    string str = "SELECT " + Columns + " FROM " + Table + (WhereClause == "" ? "" : " WHERE " + WhereClause) + " ORDER BY " + OrderBy;
                    comm.CommandText = str;
                    comm.Connection = conn;
                    da.SelectCommand = comm;
                    da.Fill(ds);
                    da.Fill(dt);
                    result.Add("DataSet", ds);
                    result.Add("DataTable", dt);
                    result.Add("DataCount", ds.Tables[0].Rows.Count);
                }
            }
            catch (Exception ex)
            {
                Utilities.LogError(ex);
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                    conn.Dispose();
                }
                ds.Dispose();
                dt.Dispose();
                da.Dispose();
                comm.Dispose();
            }
            return result;
        }

        /// <summary>
        /// Execute a query securely and return a filled hashtable
        /// </summary>
        /// <param name="Table">Table name</param>
        /// <param name="Columns">Columns</param>
        /// <param name="Where">Where hastable</param>
        /// <param name="OrderBy">Order by clause</param>
        /// <returns>Hashtable containing DataSet, DataTable and DataCount</returns>
        public static Hashtable SecureQuery(string Table, string Columns, Hashtable Where, string OrderBy)
        {
            string str = "";
            int num = 0;
            Hashtable result = new Hashtable();
            DataSet ds = new DataSet();
            DataTable dt = new DataTable();
            SqlDataAdapter da = new SqlDataAdapter();
            SqlCommand comm = new SqlCommand();
            try
            {
                using (conn = new SqlConnection(ConnectionString))
                {
                    comm.CommandText = "SELECT " + Columns + " FROM " + Table + " WHERE {0} ORDER BY " + OrderBy;
                    comm.Connection = conn;
                    foreach (string paramater in Where.Keys)
                    {
                        if (paramater[0] != '@')
                        {
                            if (num / 2 == 0)
                                str = string.Concat(new object[] { str, " ", Where["@" + num], " " });
                            str = str + "[" + paramater + "] = @" + paramater + " ";
                            num++;
                            SqlParameter param = new SqlParameter();
                            param.ParameterName = "@" + paramater;
                            param.Value = Where[paramater];
                            comm.Parameters.Add(param);
                        }
                    }
                    comm.CommandText = string.Format(comm.CommandText, str);
                    da.SelectCommand = comm;
                    da.Fill(ds);
                    da.Fill(dt);
                    result.Add("DataSet", ds);
                    result.Add("DataTable", dt);
                    result.Add("DataCount", ds.Tables[0].Rows.Count);
                }
            }
            catch (Exception ex)
            {
                Utilities.LogError(ex);
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                    conn.Dispose();
                }
                ds.Dispose();
                dt.Dispose();
                da.Dispose();
                comm.Dispose();
            }
            return result;
        }

        /// <summary>
        /// Inserts data into database and returns filled hashtable
        /// </summary>
        /// <param name="Table">Table name</param>
        /// <param name="ColumnValues">Column names and values</param>
        /// <returns>Hashtable containing Identity</returns>
        public static Hashtable InsertData(string Table, Hashtable ColumnValues)
        {
            Hashtable result = new Hashtable();
            SqlCommand comm = new SqlCommand();
            SqlCommand commIdent = new SqlCommand();
            string values = "";
            string columns = "";
            int num = 0;
            try
            {
                foreach (string parameter in ColumnValues.Keys)
                {
                    num++;
                    columns = columns + "[" + parameter + "]";
                    values = values + "@" + parameter;
                    if (num != ColumnValues.Keys.Count)
                    {
                        columns = columns + ",";
                        values = values + ",";
                    }
                    SqlParameter param = new SqlParameter();
                    param.ParameterName = "@" + parameter;
                    param.Value = ColumnValues[parameter];
                    comm.Parameters.Add(param);
                }
                comm.CommandText = "INSERT INTO " + Table + "(" + columns + ") VALUES(" + values + ")";
                comm.Connection = conn;
                comm.ExecuteNonQuery();
                commIdent.CommandText = "SELECT @@IDENTITY";
                commIdent.Connection = conn;
                int identity = Convert.ToInt32(commIdent.ExecuteScalar());
                result.Add("Identity", identity);
            }
            catch (Exception ex)
            {
                Utilities.LogError(ex);
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                    conn.Dispose();
                }
                comm.Dispose();
                commIdent.Dispose();
            }
            return result;
        }

        /// <summary>
        /// Inserts data into database if it does not already exist
        /// </summary>
        /// <param name="Table">Table name</param>
        /// <param name="ColumnValues">Column names and values</param>
        /// <param name="WhereClause">Where clause</param>
        /// <param name="IdentityColumn">Column containing identity</param>
        /// <returns>Hashtable containing Identity and Exists</returns>
        public static Hashtable InsertDataChecked(string Table, Hashtable ColumnValues, string WhereClause, string IdentityColumn)
        {
            string identity = "";
            string columns = "";
            string parameters = "";
            int num = 0;
            Hashtable result = new Hashtable();
            SqlCommand comm = new SqlCommand();
            SqlCommand commIdent = new SqlCommand();
            SqlDataReader dr;
            try
            {
                using (conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();
                    comm.CommandText = "SELECT " + IdentityColumn + " FROM " + Table + " WHERE " + WhereClause;
                    comm.Connection = conn;
                    dr = comm.ExecuteReader();
                    if (dr.Read())
                        identity = dr[IdentityColumn].ToString();
                    dr.Dispose();
                    comm = new SqlCommand();
                    if (identity == "")
                    {
                        foreach (string parameter in ColumnValues.Keys)
                        {
                            num++;
                            columns = columns + "[" + parameter + "]";
                            parameters = parameters + "@" + parameter;
                            if (num != ColumnValues.Keys.Count)
                            {
                                columns = columns + ",";
                                parameters = parameters + ",";
                            }
                            SqlParameter param = new SqlParameter();
                            param.ParameterName = "@" + parameter;
                            param.Value = ColumnValues[parameter];
                            comm.Parameters.Add(param);
                        }
                        comm.CommandText = "INSERT INTO " + Table + "(" + columns + ") VALUES(" + parameters + ")";
                        comm.Connection = conn;
                        comm.ExecuteNonQuery();
                        commIdent.CommandText = "SELECT @@IDENTITY";
                        commIdent.Connection = conn;
                        identity = commIdent.ExecuteScalar().ToString();
                        result.Add("Identity", identity);
                        result.Add("Exists", false);
                    }
                    else
                        result.Add("Exists", true);

                }
            }
            catch (Exception ex)
            {
                Utilities.LogError(ex);
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                    conn.Dispose();
                }
                comm.Dispose();
            }
            return result;
        }

        /// <summary>
        /// Update data in the database
        /// </summary>
        /// <param name="Table">Table name</param>
        /// <param name="ColumnValues">Column names and values</param>
        /// <param name="WhereClause">Where clause</param>
        public static void UpdateData(string Table, Hashtable ColumnValues, string WhereClause)
        {
            SqlCommand comm = new SqlCommand();
            string values = "";
            int num = 0;
            try
            {
                using (conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();
                    foreach (string parameter in ColumnValues.Keys)
                    {
                        num++;
                        values = values + "[" + parameter + "] = @" + parameter;
                        if (num != ColumnValues.Keys.Count)
                            values = values + ",";
                        SqlParameter param = new SqlParameter();
                        param.ParameterName = "@" + parameter;
                        param.Value = ColumnValues[parameter];
                        comm.Parameters.Add(param);
                    }
                    comm.CommandText = "UPDATE " + Table + " SET " + values + " WHERE " + WhereClause;
                    comm.Connection = conn;
                    comm.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Utilities.LogError(ex);
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                    conn.Dispose();
                }
                comm.Dispose();
            }
        }

        /// <summary>
        /// Checks if data exists in database, if it does; update, if not; insert
        /// </summary>
        /// <param name="Table">Table name</param>
        /// <param name="ColumnValues">Colum names and values</param>
        /// <param name="WhereClause">Where clause</param>
        /// <param name="IdentityColumn">column containing identity</param>
        /// <returns>Hashtable containing Identity and Exists</returns>
        public static Hashtable InsertCheckedUpdate(string Table, Hashtable ColumnValues, string WhereClause, string IdentityColumn)
        {
            throw new NotSupportedException("Not implemented yet");
        }

        /// <summary>
        /// Simple update statement
        /// </summary>
        /// <param name="Table">Table name</param>
        /// <param name="ColumnValues">Column names and values</param>
        /// <param name="WhereClause">Where clause</param>
        public static void SimpleUpdate(string Table, string ColumnValues, string WhereClause)
        {
            SqlCommand comm = new SqlCommand();
            try
            {
                using (conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();
                    comm.CommandText = "UPDATE " + Table + " SET " + ColumnValues + " WHERE " + WhereClause;
                    comm.Connection = conn;
                    comm.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Utilities.LogError(ex);
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                    conn.Dispose();
                }
                comm.Dispose();
            }
        }
    }
}
