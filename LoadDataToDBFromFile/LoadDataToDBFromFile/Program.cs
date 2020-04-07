using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoadDataToDBFromFile
{
    class Program
    {
        static void Main(string[] args)
        {
            string FilePath = @"E:\Works\LoadDataToDBFromFile\LoadDataToDBFromFile\SampleFile\SampleFeed.txt";
            DataSet ds = new DataSet();
            ds = TextfILEToDataTable(FilePath, "LoadData", "|");

            using (SqlConnection con = new SqlConnection(@"Data Source=(localdb)\v11.0;Initial Catalog=RawDataFromFile;Integrated Security=True;Connect Timeout=15;Encrypt=False;TrustServerCertificate=False"))
            {
                con.Open();
                for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
                {
                    SqlCommand cmd = new SqlCommand("INSERT INTO OffierRAWData(PEERSONID, PREFIX, FIRST_NAME,MIDDLE_NAME,LAST_NAME,DOB,CompanyIDs,Company_Names,TITLE,TITLE_ID,POSITION_TITLE,POSITIONS_START_DATE) VALUES(@PERSONID, @PREFIX, @FIRST_NAME,@MIDDLE_NAME,@LAST_NAME,@DOB,@CompanyIDs,@Company_Names,@TITLE,@TITLE_ID,@POSITION_TITLE,@POSITIONS_START_DATE)", con);
                    cmd.Parameters.AddWithValue("@PERSONID", ds.Tables[0].Rows[i]["PEERSONID"].ToString());
                    cmd.Parameters.AddWithValue("@PREFIX", ds.Tables[0].Rows[i]["PREFIX"].ToString());
                    cmd.Parameters.AddWithValue("@FIRST_NAME", ds.Tables[0].Rows[i]["FIRST_NAME"].ToString());
                    cmd.Parameters.AddWithValue("@MIDDLE_NAME", ds.Tables[0].Rows[i]["MIDDLE_NAME"].ToString());
                    cmd.Parameters.AddWithValue("@LAST_NAME", ds.Tables[0].Rows[i]["LAST_NAME"].ToString());
                    cmd.Parameters.AddWithValue("@DOB", ds.Tables[0].Rows[i]["DOB"].ToString());
                    cmd.Parameters.AddWithValue("@CompanyIDs", ds.Tables[0].Rows[i]["CompanyIDs"].ToString());
                    cmd.Parameters.AddWithValue("@Company_Names", ds.Tables[0].Rows[i]["Company_Names"].ToString());
                    cmd.Parameters.AddWithValue("@TITLE", ds.Tables[0].Rows[i]["TITLE"].ToString());
                    cmd.Parameters.AddWithValue("@TITLE_ID", ds.Tables[0].Rows[i]["TITLE_ID"].ToString());
                    cmd.Parameters.AddWithValue("@POSITION_TITLE", ds.Tables[0].Rows[i]["POSITION_TITLE"].ToString());
                    cmd.Parameters.AddWithValue("@POSITIONS_START_DATE", ds.Tables[0].Rows[i]["POSITIONS_START_DATE"].ToString());
                    cmd.ExecuteNonQuery();

                    SqlCommand cmd1 = new SqlCommand("INSERT INTO Tbl_PersonalDetails(PERSONID, PREFIX, FIRST_NAME,MIDDLE_NAME,LAST_NAME,DOB) VALUES (@PERSONID, @PREFIX, @FIRST_NAME,@MIDDLE_NAME,@LAST_NAME,@DOB)", con);
                    cmd1.Parameters.AddWithValue("@PERSONID", ds.Tables[0].Rows[i]["PEERSONID"].ToString());
                    cmd1.Parameters.AddWithValue("@PREFIX", ds.Tables[0].Rows[i]["PREFIX"].ToString());
                    cmd1.Parameters.AddWithValue("@FIRST_NAME", ds.Tables[0].Rows[i]["FIRST_NAME"].ToString());
                    cmd1.Parameters.AddWithValue("@MIDDLE_NAME", ds.Tables[0].Rows[i]["MIDDLE_NAME"].ToString());
                    cmd1.Parameters.AddWithValue("@LAST_NAME", ds.Tables[0].Rows[i]["LAST_NAME"].ToString());
                    cmd1.Parameters.AddWithValue("@DOB", ds.Tables[0].Rows[i]["DOB"].ToString());
                    cmd1.ExecuteNonQuery();

                    string[] companyID = ds.Tables[0].Rows[i]["CompanyIDs"].ToString().Split(':').ToArray();
                    string[] ComapnyName = ds.Tables[0].Rows[i]["Company_Names"].ToString().Split(':').ToArray();
                    for (int j = 0; j < companyID.Length; j++)
                    {
                        SqlCommand cmd2 = new SqlCommand("INSERT INTO Tbl_OfficeDetails(CompanyIDs, Company_Names,PersonID,OfficerID) VALUES (@CompanyIDs, @Company_Names,@PersonID,@OfficerID)", con);
                        cmd2.Parameters.AddWithValue("@CompanyIDs", companyID[j].ToString());
                        cmd2.Parameters.AddWithValue("@Company_Names", ComapnyName[j].ToString());
                        cmd2.Parameters.AddWithValue("@PersonID", ds.Tables[0].Rows[i]["PEERSONID"].ToString());
                        cmd2.Parameters.AddWithValue("@OfficerID", ds.Tables[0].Rows[i]["PEERSONID"].ToString() + companyID[j].ToString());
                        cmd2.ExecuteNonQuery();
                    }

                    SqlCommand cmd3 = new SqlCommand("INSERT INTO Tbl_OfficerTitle(TITLE, TITLE_ID,POSITION_TITLE,POSITIONS_START_DATE) VALUES (@TITLE, @TITLE_ID,@POSITION_TITLE,@POSITIONS_START_DATE)", con);
                    cmd3.Parameters.AddWithValue("@TITLE", ds.Tables[0].Rows[i]["TITLE"].ToString());
                    cmd3.Parameters.AddWithValue("@TITLE_ID", ds.Tables[0].Rows[i]["TITLE_ID"].ToString());
                    cmd3.Parameters.AddWithValue("@POSITION_TITLE", ds.Tables[0].Rows[i]["POSITION_TITLE"].ToString());
                    cmd3.Parameters.AddWithValue("@POSITIONS_START_DATE", ds.Tables[0].Rows[i]["POSITIONS_START_DATE"].ToString());
                    cmd3.ExecuteNonQuery();
                }
            }
        }

        public static DataSet TextfILEToDataTable(string File, string TableName, string delimiter)
        {
            //The DataSet to Return
            DataSet result = new DataSet();
            string[] columns = new string[] { "" };
            //Open the file in a stream reader.
            try
            {
                StreamReader streamReader = new StreamReader(File);

                //Split the first line into the columns      
                columns = streamReader.ReadLine().Split(delimiter.ToCharArray());

                //Add the new DataTable to the RecordSet
                result.Tables.Add(TableName);

                //Cycle the colums, adding those that don't exist yet
                //and sequencing the one that do.
                foreach (string col in columns)
                {
                    bool added = false;
                    string next = "";
                    int i = 0;
                    while (!added)
                    {
                        //Build the column name and remove any unwanted characters.
                        string columnname = col + next;
                        columnname = columnname.Replace("#", "");
                        columnname = columnname.Replace("'", "");
                        columnname = columnname.Replace("&", "");

                        //See if the column already exists
                        if (!result.Tables[TableName].Columns.Contains(columnname))
                        {
                            //if it doesn't then we add it here and mark it as added
                            result.Tables[TableName].Columns.Add(columnname.Trim());
                            added = true;
                        }
                        else
                        {
                            //if it did exist then we increment the sequencer and try again.
                            i++;
                            next = "_" + i.ToString();
                        }
                    }
                }


                //Read the rest of the data in the file.        
                string AllData = streamReader.ReadToEnd();

                string[] rows = AllData.Split("\r\n".ToCharArray());
                streamReader.Close();
                streamReader.Dispose();
                //Now add each row to the DataSet        
                foreach (string r in rows)
                {
                    //Split the row at the delimiter.
                    if (r.Length > 1)
                    {
                        string[] items = r.Split(delimiter.ToCharArray());
                        //Add the item
                        result.Tables[TableName].Rows.Add(items);
                    }
                }
            }
            catch (Exception ex)
            {
            }

            return result;
        }
    }
}

