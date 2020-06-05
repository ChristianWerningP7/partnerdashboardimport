using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data.SqlClient;
using System.Data;
using System.Runtime.InteropServices;
using System.Security.Principal;
using System.Security.Permissions;
using Microsoft.Win32.SafeHandles;
using System.Runtime.ConstrainedExecution;
using System.Security;
using System.Net.Mail;
using System.Net;
using PartnerDashboardImport.SF;
using Microsoft.Exchange.WebServices.Data;
using Attachment = Microsoft.Exchange.WebServices.Data.Attachment;
using Microsoft.VisualBasic.FileIO;

namespace PartnerDashboardImport
{
    class Program
    {
        private static bool lerr = false;
        private static FileStream logstream = null;
        private static string strI = String.Empty;
        private const string ConStr = @"Server=ABGISDB05A.abg.fsc.net;Initial Catalog=fts_cm;User ID=www_fts_cm;Password=Aa1lsadiwzeranbsdka$";
        private const string ConStrEvt = @"Server=ABGISDB04A.abg.fsc.net;Initial Catalog=de_eventkalender;User ID=de_event;Password=Ev3ntk4l3nder";
        private static SforceService SfdcBinding { get; set; }

        [DllImport("advapi32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        public static extern bool LogonUser(String lpszUsername, String lpszDomain, String lpszPassword, int dwLogonType, int dwLogonProvider, out SafeTokenHandle phToken);

        [DllImport("kernel32.dll", CharSet = CharSet.Auto)]
        public extern static bool CloseHandle(IntPtr handle);
        [PermissionSetAttribute(SecurityAction.Demand, Name = "FullTrust")]

        static void Main(string[] args)
        {

            ServicePointManager.Expect100Continue = true;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Ssl3 | SecurityProtocolType.Tls | (SecurityProtocolType)3072 | (SecurityProtocolType)768;
            ServicePointManager.DefaultConnectionLimit = 9999;
            ServicePointManager.ServerCertificateValidationCallback = (sender, certificate, chain, sslPolicyErrors) => true;
            DateTime da = DateTime.Today;
            strI += "Start Import : " + DateTime.Now.ToLongTimeString() + Environment.NewLine;

            try
            {
                logstream = new FileStream(@"C:\tasks\PartnerDashboardImport\log\log_import_pbr_" + da.ToString("ddMMyyyy") + ".txt", FileMode.Append);
                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(strI), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(strI));
                logstream.Flush();
                //read all Trust/Debitorids from db
                Dictionary<string, string> alldebitorids = new Dictionary<string, string>();
                using (SqlConnection sqlConna = new SqlConnection(ConStr))
                {
                    try
                    {

                        string strsql = "select DebitorID,TrustID from  PartnerDashboard_TrustDebitor";
                        using (SqlCommand sqlComma = new SqlCommand(strsql, sqlConna))
                        {
                            if (sqlConna.State != ConnectionState.Open)
                            {
                                sqlConna.Open();
                            }
                            using (SqlDataReader dr = sqlComma.ExecuteReader())
                            {
                                while (dr.Read())
                                {
                                    alldebitorids.Add(dr[0].ToString(), dr[1].ToString());
                                }

                            }

                        }

                    }
                    catch (Exception err)
                    {
                        String sErr = "y2.5x" + err.Message + err.StackTrace;
                        logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                        logstream.Flush();
                        lerr = true;
                    }
                    finally
                    {
                        sqlConna.Close();
                    }
                }
                DataTable experttypes = new DataTable();

                using (SqlConnection sqlConna = new SqlConnection(ConStr))
                {
                    string strsql = "select ExpertTitle,ExpertID, CertificationPart, certificationtitle from PartnerDashboard_ExpertType et left join PartnerDashboard_certificationtype ct on certificationpart=certificationid  ";
                    using (SqlDataAdapter ad = new SqlDataAdapter(strsql, sqlConna))
                    {
                        if (sqlConna.State != ConnectionState.Open)
                        {
                            sqlConna.Open();
                        }
                        ad.Fill(experttypes);
                    }
                }



                Dictionary<string, List<string>> ext = new Dictionary<string, List<string>>();
                foreach (DataRow etr in experttypes.Rows)
                {

                    if (ext.Keys.Contains(etr["Expertid"].ToString()))
                    {

                        ext[etr["Expertid"].ToString()].Add(etr["Certificationtitle"].ToString());
                    }
                    else
                    {
                        List<string> newlist = new List<string>();
                        newlist.Add(etr["Certificationtitle"].ToString());
                        ext[etr["Expertid"].ToString()] = newlist;
                    }

                }


                //get certificationdata from Exchange
                bool isTest = true;
                DataTable accountcerts = null;
                DataTable contactcerts = null;
                DataTable missingcompanycerts = null;
                if (!isTest)
                {
                    accountcerts = new DataTable();
                    contactcerts = new DataTable();
                    missingcompanycerts = new DataTable();
                    //accountcerts = GetDataFromExchange("Your #1c- Partner Certifications- Channel Partner report");
                    //contactcerts = GetDataFromExchange("Your #17- Profile Certifications listed by partners- Channel Partner report");
                    //missingcompanycerts = GetDataFromExchange("Your #4b - Missing profile certifications for partner certification- Channel Partner report");
                }
                else
                {
                    accountcerts = GetDataFromLocal("Your Channel partner- Partner certifications #1cReport");
                    contactcerts = GetDataFromLocal("Your #17 Profile Certification Report");
                    missingcompanycerts = GetDataFromLocal("Your Channel Partner- missing profile Certifications Report # 4b");
                }
                DataTable zerts = new DataTable();
                zerts.Columns.Add("TrustID", typeof(string));
                zerts.Columns.Add("DebitorID", typeof(string));
                zerts.Columns.Add("ExpertID", typeof(string));
                zerts.Columns.Add("ExpertTitle", typeof(string));
                zerts.Columns.Add("CertificationPart", typeof(string));
                zerts.Columns.Add("CertificationTitle", typeof(string));
                zerts.Columns.Add("Contactid", typeof(string));
                zerts.Columns.Add("ContactFirstname", typeof(string));
                zerts.Columns.Add("ContactLastname", typeof(string));
                zerts.Columns.Add("Startdate", typeof(string));
                zerts.Columns.Add("Enddate", typeof(string));
                zerts.Columns.Add("Missing", typeof(string));

                foreach (DataRow row in accountcerts.Rows)
                {
                    if (row["Country"].Equals("Germany") || row["Country"].Equals("Austria") || row["Country"].Equals("Switzerland"))
                    {
                        DateTime zertstart, zertend;
                        if (DateTime.TryParse(row["Certified date"].ToString(), out zertstart) && DateTime.TryParse(row["End date"].ToString(), out zertend))
                        {
                            string pn = row["Partner nr"].ToString().Trim();
                            if (!String.IsNullOrEmpty(pn) && alldebitorids.ContainsKey(pn))
                            {
                                DataRow[] expert = experttypes.Select(" ExpertTitle = '" + row["Certification"].ToString() + "'");
                                if (expert.Count() > 0)
                                {
                                    DataRow newzertrow = zerts.NewRow();
                                    newzertrow["TrustID"] = alldebitorids[pn];
                                    newzertrow["DebitorID"] = pn;
                                    newzertrow["ExpertID"] = expert[0]["ExpertID"].ToString();
                                    newzertrow["ExpertTitle"] = row["Certification"].ToString().Trim();
                                    newzertrow["CertificationPart"] = String.Empty;
                                    newzertrow["CertificationTitle"] = String.Empty;
                                    newzertrow["Contactid"] = String.Empty;
                                    newzertrow["ContactFirstname"] = String.Empty;
                                    newzertrow["ContactLastname"] = String.Empty;
                                    newzertrow["Startdate"] = row["Certified date"].ToString();
                                    newzertrow["Enddate"] = row["End date"].ToString();
                                    zerts.Rows.Add(newzertrow);
                                }

                            }
                        }
                    }
                }
                String errstrx2 = DateTime.Now.ToLongTimeString() + " zu speichernde PartnerDashboard-Accountzertifizieruns-Datensätze: " + zerts.Rows.Count.ToString() + Environment.NewLine;
                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstrx2), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstrx2));
                logstream.Flush();
                strI += errstrx2;
                foreach (DataRow row in contactcerts.Rows)
                {
                    if (row["Country"].Equals("Germany") || row["Country"].Equals("Austria") || row["Country"].Equals("Switzerland"))
                    {
                        DateTime zertstart, zertend;
                        if (DateTime.TryParse(row["Certified date"].ToString(), out zertstart) && DateTime.TryParse(row["End date"].ToString(), out zertend))
                        {

                            string pn = row["Partner nr"].ToString().Trim();
                            if (!String.IsNullOrEmpty(pn) && alldebitorids.ContainsKey(pn))
                            {
                                DataRow[] expert = experttypes.Select(" certificationtitle = '" + row["Profilcertification"].ToString() + "'");

                                foreach (DataRow exrow in expert)
                                {

                                    DataRow newzertrow = zerts.NewRow();
                                    newzertrow["TrustID"] = alldebitorids[pn];
                                    newzertrow["DebitorID"] = pn;
                                    newzertrow["ExpertID"] = exrow["ExpertID"].ToString();
                                    newzertrow["ExpertTitle"] = exrow["ExpertTitle"].ToString();
                                    newzertrow["CertificationPart"] = exrow["CertificationPart"].ToString();
                                    newzertrow["CertificationTitle"] = row["Profilcertification"].ToString();
                                    newzertrow["Contactid"] = row["Contact nr"].ToString();
                                    newzertrow["ContactFirstname"] = row["Firstname"].ToString();
                                    newzertrow["ContactLastname"] = row["Lastname"].ToString();
                                    newzertrow["Startdate"] = row["Certified date"].ToString();
                                    newzertrow["Enddate"] = row["End date"].ToString();
                                    zerts.Rows.Add(newzertrow);
                                }

                            }

                        }

                    }
                }
                errstrx2 = DateTime.Now.ToLongTimeString() + " zu speichernde PartnerDashboard-Account- und Contactzertifizieruns-Datensätze: " + zerts.Rows.Count.ToString() + Environment.NewLine;
                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstrx2), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstrx2));
                logstream.Flush();
                strI += errstrx2;

                foreach (DataRow row in missingcompanycerts.Rows)
                {
                    if (row["Country"].Equals("Germany") || row["Country"].Equals("Austria") || row["Country"].Equals("Switzerland"))
                    {

                        string pn = row["Partner nr"].ToString().Trim();
                        if (!String.IsNullOrEmpty(pn) && alldebitorids.ContainsKey(pn))
                        {
                            // DataRow[] expert = experttypes.Select(" certificationtitle = '" + row["Profilcertification"].ToString() + "'");

                            string missingCert = row["Missing company cert prerequisites"].ToString();
                            DataRow certRow = GetRowByNrAndDebitorId(zerts.Rows, pn, alldebitorids[pn]);
                            if (certRow != null)
                            {
                                certRow["Missing"] = missingCert;
                            }
                        }
                    }
                }
                errstrx2 = DateTime.Now.ToLongTimeString() + " zu speichernde PartnerDashboard-Account- und Contactzertifizieruns-Datensätze: " + zerts.Rows.Count.ToString() + Environment.NewLine;
                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstrx2), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstrx2));
                logstream.Flush();
                strI += errstrx2;


                if (zerts.Rows.Count > 0)
                {
                    SqlConnection sqlConna = new SqlConnection(ConStr);
                    try
                    {
                        SqlCommand sqlComma = new SqlCommand();
                        sqlComma.Connection = sqlConna;
                        sqlConna.Open();
                        string strsql = "delete from PartnerDashboard_Certifications ";
                        if (accountcerts.Rows.Count == 0)
                        {
                            strsql += " where isnull(CertificationTitle,'') <> '' ";
                        }
                        else if (contactcerts.Rows.Count == 0)
                        {
                            strsql += " where isnull(CertificationTitle,'') = '' ";
                        }




                        sqlComma.CommandText = strsql;
                        sqlComma.ExecuteNonQuery();


                        using (SqlBulkCopy sqlBulk = new SqlBulkCopy(ConStr))
                        {
                            try
                            {
                                sqlBulk.BatchSize = 10000;
                                sqlBulk.NotifyAfter = zerts.Rows.Count;
                                sqlBulk.DestinationTableName = "PartnerDashboard_Certifications";



                                sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("TrustID", "TrustID"));
                                sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("DebitorID", "DebitorID"));
                                sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ExpertID", "ExpertID"));
                                sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ExpertTitle", "ExpertTitle"));
                                sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("CertificationPart", "CertificationPart"));
                                sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("CertificationTitle", "CertificationTitle"));
                                sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("Contactid", "Contactid"));
                                sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ContactFirstname", "ContactFirstname"));
                                sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ContactLastname", "ContactLastname"));
                                sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("Startdate", "Startdate"));
                                sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("Enddate", "Enddate"));
                                sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("Missing", "Missing"));

                                sqlBulk.WriteToServer(zerts);
                            }
                            catch (Exception e1)
                            {
                                String sErr = "Certifications Bulkcopy: " + e1.Message + e1.StackTrace;
                                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                logstream.Flush();
                                lerr = true;
                            }
                        }

                    }
                    catch (Exception e1)
                    {
                        String sErr = "Certifications : " + e1.Message + e1.StackTrace;
                        logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                        logstream.Flush();
                        lerr = true;
                    }
                }

            }
            catch (Exception e1)
            {
                String sErr = "Certifications (gesamt) : " + e1.Message + e1.StackTrace;
                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                logstream.Flush();
                lerr = true;
            }

            /*************************Impersonate***********************/
            try
            {
                SafeTokenHandle safeTokenHandle;

                const int LOGON32_PROVIDER_DEFAULT = 0;
                //This parameter causes LogonUser to create a primary token. 
                // const int LOGON32_LOGON_INTERACTIVE = 2;
                const int LOGON32_LOGON_NEW_CREDENTIALS = 9;

                bool returnValue = LogonUser("cpp-de-nl-p7", "G02", "eywAGC695(&*", LOGON32_LOGON_NEW_CREDENTIALS, LOGON32_PROVIDER_DEFAULT, out safeTokenHandle);

                // bool returnValue = LogonUser("cpp-de-nl-p7", "Domfsc01", "", LOGON32_LOGON_NEW_CREDENTIALS, LOGON32_PROVIDER_DEFAULT, out safeTokenHandle);

                if (false == returnValue)
                {
                    int ret = Marshal.GetLastWin32Error();
                    strI += "LogonUser failed with error code : " + ret + Environment.NewLine;
                    throw new System.ComponentModel.Win32Exception(ret);
                }
                using (safeTokenHandle)
                {
                    // strI += "Did LogonUser Succeed? " + (returnValue ? "Yes" : "No") + Environment.NewLine;

                    using (WindowsImpersonationContext impersonatedUser = WindowsIdentity.Impersonate(safeTokenHandle.DangerousGetHandle()))
                    {

                        /*************************Impersonated Actions Start***********************/


                        try
                        {
                            //partner revenue debitor 
                            try
                            {
                                //revnue
                                string[] arrges = System.IO.File.ReadAllLines(@"\\G02DEAS01PARE\Public7\CPP_Revenue_PartnerReport.csv", Encoding.GetEncoding("ISO-8859-1"));
                                string FY = DateTime.Now.AddDays(-10).Year.ToString();
                                string firstline = arrges[0];
                                if (firstline.Length > 2)
                                {
                                    string[] linearr = firstline.Substring(1, firstline.Length - 2).Split(new string[] { ";" }, StringSplitOptions.None);
                                    string erg = linearr[18].Trim().Replace("RevenueFYTotal", "");
                                    if (erg.Length == 4)
                                    {
                                        FY = erg;
                                    }
                                }
                                //DataTable acc_tab = new DataTable();
                                String errstrx1 = DateTime.Now.ToLongTimeString() + " anzahl gelesener PartnerDashboard-Revenue-Datensätze: " + arrges.Length.ToString() + Environment.NewLine;
                                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstrx1), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstrx1));
                                logstream.Flush();
                                strI += errstrx1;


                                try
                                {
                                    DataTable pb_revenue = new DataTable();
                                    pb_revenue.Columns.Add("ResellerGroupID", typeof(string));
                                    pb_revenue.Columns.Add("ResellerGroupName", typeof(string));
                                    pb_revenue.Columns.Add("ResellerDebitorID", typeof(string));
                                    pb_revenue.Columns.Add("ResellerDebitorNr", typeof(string));
                                    pb_revenue.Columns.Add("ResellerDebitorName", typeof(string));
                                    pb_revenue.Columns.Add("ProductLineFullText", typeof(string));
                                    pb_revenue.Columns.Add("RevenueFY04", typeof(string));
                                    pb_revenue.Columns.Add("RevenueFY05", typeof(string));
                                    pb_revenue.Columns.Add("RevenueFY06", typeof(string));
                                    pb_revenue.Columns.Add("RevenueFY07", typeof(string));
                                    pb_revenue.Columns.Add("RevenueFY08", typeof(string));
                                    pb_revenue.Columns.Add("RevenueFY09", typeof(string));
                                    pb_revenue.Columns.Add("RevenueFY10", typeof(string));
                                    pb_revenue.Columns.Add("RevenueFY11", typeof(string));
                                    pb_revenue.Columns.Add("RevenueFY12", typeof(string));
                                    pb_revenue.Columns.Add("RevenueFY01", typeof(string));
                                    pb_revenue.Columns.Add("RevenueFY02", typeof(string));
                                    pb_revenue.Columns.Add("RevenueFY03", typeof(string));
                                    pb_revenue.Columns.Add("RevenueFYTotal", typeof(string));


                                    for (int i = 2; i < arrges.Length; i++)
                                    {

                                        string line = arrges[i];
                                        if (line.Length > 2)
                                        {
                                            string[] linearr = line.Substring(1, line.Length - 2).Split(new string[] { ";" }, StringSplitOptions.None);
                                            //"Account.Id"	"Account.Name"	"Account.Account_ID__c"	"Account.SAP_Id__c"	"Account.BillingStreet"	"Account.BillingCity"	"Account.BillingPostalCode"	"Account.BillingCountry"	"Account.eMail_Address__c"	"Account.Owner.Domain_Account__c"	"Account.Organization__c.Name"	"Account.Sales_Area__c.Name"	"Account.Sales_Region__c.Name"	"Account.Partner__c"	"Account.Group_Name__c"	"Account.SPP_LEVEL_Achieved__c"	"Account.SPP_LEVEL_Committed__c"	"Account.International_Channel_Partner__c"	"Account.Partner_Status__c"	"Account.Channel_Segment__c"	"Account.Local_Channel_Seg__c"	"Account.Account_Segment__c"	"Account.LastModifiedDate"


                                            if (String.IsNullOrEmpty(line))
                                            {
                                                String errstr1 = "leere Zeile" + Environment.NewLine + Environment.NewLine;
                                                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstr1), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstr1));
                                                logstream.Flush();
                                                //logstr += errstr1;
                                            }

                                            if (linearr.Length != 19)
                                            {

                                                String errstr = linearr.Length + " : " + line + Environment.NewLine + Environment.NewLine;
                                                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstr));
                                                logstream.Flush();
                                                //logstr += errstr;
                                            }
                                            else
                                            {
                                                try
                                                {
                                                    object[] rowarr = new object[19];
                                                    rowarr[0] = linearr[0].ToString().Trim();
                                                    rowarr[1] = linearr[1].ToString().Trim();
                                                    rowarr[2] = linearr[2].ToString().Trim();
                                                    rowarr[3] = linearr[3].ToString().Trim();
                                                    rowarr[4] = linearr[4].ToString().Trim();
                                                    rowarr[5] = linearr[5].ToString().Trim();
                                                    rowarr[6] = linearr[6].ToString().Trim();
                                                    rowarr[7] = linearr[7].ToString().Trim();
                                                    rowarr[8] = linearr[8].ToString().Trim();
                                                    rowarr[9] = linearr[9].ToString().Trim();
                                                    rowarr[10] = linearr[10].ToString().Trim();
                                                    rowarr[11] = linearr[11].ToString().Trim();
                                                    rowarr[12] = linearr[12].ToString().Trim();
                                                    rowarr[13] = linearr[13].ToString().Trim();
                                                    rowarr[14] = linearr[14].ToString().Trim();
                                                    rowarr[15] = linearr[15].ToString().Trim();
                                                    rowarr[16] = linearr[16].ToString().Trim();
                                                    rowarr[17] = linearr[17].ToString().Trim();
                                                    rowarr[18] = linearr[18].ToString().Trim();



                                                    pb_revenue.Rows.Add(rowarr);

                                                }
                                                catch (Exception se)
                                                {
                                                    String sErr = "x" + se.Message + se.StackTrace + Environment.NewLine;
                                                    logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                                    logstream.Flush();
                                                    lerr = true;

                                                }

                                            }

                                        }

                                        arrges[i] = null;
                                    }

                                    //partnerbonus revenue resellergroup

                                    string[] arrgesgr = System.IO.File.ReadAllLines(@"\\G02DEAS01PARE\Public7\CPP_Revenue_PartnerReportByGroup.csv", Encoding.GetEncoding("ISO-8859-1"));
                                    //DataTable acc_tab = new DataTable();
                                    String errstrx1gr = DateTime.Now.ToLongTimeString() + " anzahl gelesener PartnerDashboard-RevenueByGroup-Datensätze: " + arrgesgr.Length.ToString() + Environment.NewLine + "FY: " + FY + Environment.NewLine;
                                    logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstrx1gr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstrx1gr));
                                    logstream.Flush();
                                    strI += errstrx1gr;


                                    try
                                    {



                                        for (int i = 2; i < arrgesgr.Length; i++)
                                        {

                                            string line = arrgesgr[i];
                                            if (line.Length > 2)
                                            {
                                                string[] linearr = line.Substring(1, line.Length - 2).Split(new string[] { ";" }, StringSplitOptions.None);
                                                //"Account.Id"	"Account.Name"	"Account.Account_ID__c"	"Account.SAP_Id__c"	"Account.BillingStreet"	"Account.BillingCity"	"Account.BillingPostalCode"	"Account.BillingCountry"	"Account.eMail_Address__c"	"Account.Owner.Domain_Account__c"	"Account.Organization__c.Name"	"Account.Sales_Area__c.Name"	"Account.Sales_Region__c.Name"	"Account.Partner__c"	"Account.Group_Name__c"	"Account.SPP_LEVEL_Achieved__c"	"Account.SPP_LEVEL_Committed__c"	"Account.International_Channel_Partner__c"	"Account.Partner_Status__c"	"Account.Channel_Segment__c"	"Account.Local_Channel_Seg__c"	"Account.Account_Segment__c"	"Account.LastModifiedDate"



                                                if (String.IsNullOrEmpty(line))
                                                {
                                                    String errstr1 = "leere Zeile" + Environment.NewLine + Environment.NewLine;
                                                    logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstr1), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstr1));
                                                    logstream.Flush();
                                                    //logstr += errstr1;
                                                }

                                                if (linearr.Length != 16)
                                                {

                                                    String errstr = linearr.Length + " : " + line + Environment.NewLine + Environment.NewLine;
                                                    logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstr));
                                                    logstream.Flush();
                                                    //logstr += errstr;
                                                }
                                                else
                                                {
                                                    try
                                                    {
                                                        object[] rowarr = new object[19];
                                                        rowarr[0] = linearr[0].ToString().Trim();
                                                        rowarr[1] = linearr[1].ToString().Trim();
                                                        rowarr[2] = "";
                                                        rowarr[3] = "";
                                                        rowarr[4] = "";
                                                        rowarr[5] = linearr[2].ToString().Trim();
                                                        rowarr[6] = linearr[3].ToString().Trim();
                                                        rowarr[7] = linearr[4].ToString().Trim();
                                                        rowarr[8] = linearr[5].ToString().Trim();
                                                        rowarr[9] = linearr[6].ToString().Trim();
                                                        rowarr[10] = linearr[7].ToString().Trim();
                                                        rowarr[11] = linearr[8].ToString().Trim();
                                                        rowarr[12] = linearr[9].ToString().Trim();
                                                        rowarr[13] = linearr[10].ToString().Trim();
                                                        rowarr[14] = linearr[11].ToString().Trim();
                                                        rowarr[15] = linearr[12].ToString().Trim();
                                                        rowarr[16] = linearr[13].ToString().Trim();
                                                        rowarr[17] = linearr[14].ToString().Trim();
                                                        rowarr[18] = linearr[15].ToString().Trim();



                                                        pb_revenue.Rows.Add(rowarr);

                                                    }
                                                    catch (Exception se)
                                                    {
                                                        String sErr = "x" + se.Message + se.StackTrace + Environment.NewLine;
                                                        logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                                        logstream.Flush();
                                                        lerr = true;

                                                    }

                                                }

                                            }

                                            arrges[i] = null;
                                        }
                                    }
                                    catch (Exception e1)
                                    {
                                        String errstr = DateTime.Now.ToLongTimeString() + ": Fehler(2) Import PartnerBonusRevenue : " + e1 + Environment.NewLine;
                                        logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstr));
                                        logstream.Flush();

                                        lerr = true;
                                    }


                                    string errst1 = DateTime.Now.ToLongTimeString() + " zu speichernde PartnerDashboard-Revenue-Datensätze: " + pb_revenue.Rows.Count.ToString() + Environment.NewLine;
                                    logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errst1), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errst1));
                                    logstream.Flush();
                                    strI += errst1;

                                    if (pb_revenue.Rows.Count > 0)
                                    {
                                        SqlConnection sqlConna = new SqlConnection(ConStr);
                                        try
                                        {
                                            SqlCommand sqlComma = new SqlCommand();
                                            sqlComma.Connection = sqlConna;
                                            string strsql = "update PartnerDashboard_Parameter set value=@FY where name='FY' ";
                                            bool ok = true;
                                            try
                                            {
                                                if (sqlConna.State != ConnectionState.Open)
                                                {
                                                    sqlConna.Open();
                                                }

                                                sqlComma.CommandText = strsql;
                                                sqlComma.Parameters.Add(new SqlParameter("@FY", SqlDbType.VarChar, 200));
                                                sqlComma.Parameters["@FY"].Value = FY;
                                                sqlComma.ExecuteNonQuery();
                                            }
                                            catch (Exception err)
                                            {
                                                String sErr = "y2.1.1" + err.Message + err.StackTrace;
                                                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                                logstream.Flush();
                                                lerr = true;
                                                ok = false;
                                            }
                                            sqlComma.Parameters.Clear();
                                            try
                                            {
                                                if (sqlConna.State != ConnectionState.Open) sqlConna.Open();

                                                strsql = "delete from PartnerDashboard_Revenue ";
                                                sqlComma.CommandTimeout = 120;
                                                sqlComma.CommandText = strsql;
                                                sqlComma.ExecuteNonQuery();
                                            }
                                            catch (Exception err)
                                            {
                                                ok = false;
                                                String sErr = "y2.1.2" + err.Message + err.StackTrace;
                                                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                                logstream.Flush();
                                                lerr = true;
                                            }
                                            if (ok)
                                            {
                                                using (SqlBulkCopy sqlBulk = new SqlBulkCopy(ConStr))
                                                {
                                                    try
                                                    {
                                                        sqlBulk.BatchSize = 10000;
                                                        sqlBulk.NotifyAfter = pb_revenue.Rows.Count;
                                                        sqlBulk.DestinationTableName = "PartnerDashboard_Revenue";

                                                        sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ResellerGroupID", "ResellerGroupID"));
                                                        sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ResellerGroupName", "ResellerGroupName"));
                                                        sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ResellerDebitorID", "ResellerDebitorID"));
                                                        sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ResellerDebitorNr", "ResellerDebitorNr"));
                                                        sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ResellerDebitorName", "ResellerDebitorName"));
                                                        sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ProductLineFullText", "ProductLineFullText"));
                                                        sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("RevenueFY04", "RevenueFY04"));
                                                        sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("RevenueFY05", "RevenueFY05"));
                                                        sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("RevenueFY06", "RevenueFY06"));
                                                        sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("RevenueFY07", "RevenueFY07"));
                                                        sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("RevenueFY08", "RevenueFY08"));
                                                        sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("RevenueFY09", "RevenueFY09"));
                                                        sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("RevenueFY10", "RevenueFY10"));
                                                        sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("RevenueFY11", "RevenueFY11"));
                                                        sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("RevenueFY12", "RevenueFY12"));
                                                        sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("RevenueFY01", "RevenueFY01"));
                                                        sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("RevenueFY02", "RevenueFY02"));
                                                        sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("RevenueFY03", "RevenueFY03"));
                                                        sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("RevenueFYTotal", "RevenueFYTotal"));

                                                        sqlBulk.WriteToServer(pb_revenue);
                                                    }
                                                    catch (Exception e1)
                                                    {
                                                        String sErr = "Revenues Bulkcopy: " + e1.Message + e1.StackTrace;
                                                        logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                                        logstream.Flush();
                                                        lerr = true;
                                                    }
                                                }
                                            }



                                        }
                                        catch (Exception err)
                                        {
                                            String sErr = "y2.1" + err.Message + err.StackTrace;
                                            logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                            logstream.Flush();
                                            lerr = true;
                                        }
                                        finally
                                        {
                                            sqlConna.Close();
                                        }
                                    }
                                }
                                catch (Exception err)
                                {

                                    String sErr = "y3.1" + err.Message + err.StackTrace;
                                    logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                    logstream.Flush();
                                    lerr = true;
                                }
                                finally
                                {

                                }



                            }
                            catch (Exception e1)
                            {
                                String errstr = DateTime.Now.ToLongTimeString() + ": Fehler(2) Import PartnerBonusRevenue : " + e1 + Environment.NewLine;
                                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstr));
                                logstream.Flush();

                                lerr = true;
                            }

                            //partnerbonus data
                            try
                            {
                                //
                                string[] arrges = System.IO.File.ReadAllLines(@"\\G02DEAS01PARE\Public7\CPP_Bonusdarstellung_PartnerReport.csv", Encoding.GetEncoding("ISO-8859-1"));
                                //DataTable acc_tab = new DataTable();
                                String errstrx1 = DateTime.Now.ToLongTimeString() + " anzahl gelesener PartnerDashboard-Bonusdarstellung-Datensätze: " + arrges.Length.ToString() + Environment.NewLine;
                                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstrx1), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstrx1));
                                logstream.Flush();
                                strI += errstrx1;


                                try
                                {
                                    DataTable pb_bonusdata = new DataTable();

                                    pb_bonusdata.Columns.Add("ResellerGroupID", typeof(string));
                                    pb_bonusdata.Columns.Add("ResellerGroupName", typeof(string));
                                    pb_bonusdata.Columns.Add("Bonusvertragsnummer", typeof(string));
                                    pb_bonusdata.Columns.Add("Vertragslaufzeit", typeof(string));
                                    pb_bonusdata.Columns.Add("Bonusziel", typeof(string));
                                    pb_bonusdata.Columns.Add("UmsatzErfuellung", typeof(string));
                                    pb_bonusdata.Columns.Add("AnrechenbarerUmsatz", typeof(string));
                                    pb_bonusdata.Columns.Add("BonusanspruchEUR", typeof(string));
                                    pb_bonusdata.Columns.Add("Bonuserfuellung", typeof(string));
                                    pb_bonusdata.Columns.Add("Bonustyp", typeof(string));



                                    for (int i = 2; i < arrges.Length; i++)
                                    {

                                        string line = arrges[i];
                                        if (line.Length > 2)
                                        {
                                            string[] linearr = line.Substring(1, line.Length - 2).Split(new string[] { ";" }, StringSplitOptions.None);
                                            //"Account.Id"	"Account.Name"	"Account.Account_ID__c"	"Account.SAP_Id__c"	"Account.BillingStreet"	"Account.BillingCity"	"Account.BillingPostalCode"	"Account.BillingCountry"	"Account.eMail_Address__c"	"Account.Owner.Domain_Account__c"	"Account.Organization__c.Name"	"Account.Sales_Area__c.Name"	"Account.Sales_Region__c.Name"	"Account.Partner__c"	"Account.Group_Name__c"	"Account.SPP_LEVEL_Achieved__c"	"Account.SPP_LEVEL_Committed__c"	"Account.International_Channel_Partner__c"	"Account.Partner_Status__c"	"Account.Channel_Segment__c"	"Account.Local_Channel_Seg__c"	"Account.Account_Segment__c"	"Account.LastModifiedDate"



                                            if (String.IsNullOrEmpty(line))
                                            {
                                                String errstr1 = "leere Zeile" + Environment.NewLine + Environment.NewLine;
                                                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstr1), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstr1));
                                                logstream.Flush();
                                                //logstr += errstr1;
                                            }

                                            if (linearr.Length != 10)
                                            {

                                                String errstr = linearr.Length + " : " + line + Environment.NewLine + Environment.NewLine;
                                                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstr));
                                                logstream.Flush();
                                                //logstr += errstr;
                                            }
                                            else
                                            {
                                                try
                                                {
                                                    object[] rowarr = new object[10];
                                                    rowarr[0] = linearr[0].ToString().Trim();
                                                    rowarr[1] = linearr[1].ToString().Trim();
                                                    rowarr[2] = linearr[2].ToString().Trim();
                                                    rowarr[3] = linearr[3].ToString().Trim();
                                                    rowarr[4] = linearr[4].ToString().Trim();
                                                    rowarr[5] = linearr[5].ToString().Trim();
                                                    rowarr[6] = linearr[6].ToString().Trim();
                                                    rowarr[7] = linearr[7].ToString().Trim();
                                                    rowarr[8] = linearr[8].ToString().Trim();
                                                    rowarr[9] = linearr[9].ToString().Trim();


                                                    pb_bonusdata.Rows.Add(rowarr);

                                                }
                                                catch (Exception se)
                                                {
                                                    String sErr = "x" + se.Message + se.StackTrace + Environment.NewLine;
                                                    logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                                    logstream.Flush();
                                                    lerr = true;

                                                }

                                            }

                                        }

                                        arrges[i] = null;
                                    }
                                    string errst2 = DateTime.Now.ToLongTimeString() + " zu speichernde PartnerDashboard-Bonusdata-Datensätze: " + pb_bonusdata.Rows.Count.ToString() + Environment.NewLine;
                                    logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errst2), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errst2));
                                    logstream.Flush();
                                    strI += errst2;

                                    if (pb_bonusdata.Rows.Count > 0)
                                    {
                                        SqlConnection sqlConna = new SqlConnection(ConStr);
                                        try
                                        {
                                            SqlCommand sqlComma = new SqlCommand();
                                            sqlComma.Connection = sqlConna;
                                            sqlConna.Open();
                                            string strsql = "delete from PartnerDashboard_Bonusdata ";

                                            sqlComma.CommandText = strsql;
                                            sqlComma.ExecuteNonQuery();


                                            using (SqlBulkCopy sqlBulk = new SqlBulkCopy(ConStr))
                                            {
                                                try
                                                {
                                                    sqlBulk.BatchSize = 10000;
                                                    sqlBulk.NotifyAfter = pb_bonusdata.Rows.Count;
                                                    sqlBulk.DestinationTableName = "PartnerDashboard_Bonusdata";



                                                    sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ResellerGroupID", "ResellerGroupID"));
                                                    sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ResellerGroupName", "ResellerGroupName"));
                                                    sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("Bonusvertragsnummer", "Bonusvertragsnummer"));
                                                    sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("Vertragslaufzeit", "Vertragslaufzeit"));
                                                    sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("Bonusziel", "Bonusziel"));
                                                    sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("UmsatzErfuellung", "UmsatzErfuellung"));
                                                    sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("AnrechenbarerUmsatz", "AnrechenbarerUmsatz"));
                                                    sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("BonusanspruchEUR", "BonusanspruchEUR"));
                                                    sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("Bonuserfuellung", "Bonuserfuellung"));
                                                    sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("Bonustyp", "Bonustyp"));

                                                    sqlBulk.WriteToServer(pb_bonusdata);
                                                }
                                                catch (Exception e1)
                                                {
                                                    String sErr = "Bonusdata Bulkcopy: " + e1.Message + e1.StackTrace;
                                                    logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                                    logstream.Flush();
                                                    lerr = true;
                                                }
                                            }





                                        }
                                        catch (Exception err)
                                        {
                                            String sErr = "y2.2" + err.Message + err.StackTrace;
                                            logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                            logstream.Flush();
                                            lerr = true;
                                        }
                                        finally
                                        {
                                            sqlConna.Close();
                                        }
                                    }
                                }
                                catch (Exception err)
                                {

                                    String sErr = "y3.2" + err.Message + err.StackTrace;
                                    logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                    logstream.Flush();
                                    lerr = true;
                                }
                                finally
                                {

                                }
                            }
                            catch (Exception e1)
                            {
                                String errstr = DateTime.Now.ToLongTimeString() + ": Fehler(2b) Import PartnerBonus Bonusdata : " + e1 + Environment.NewLine;
                                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstr));
                                logstream.Flush();

                                lerr = true;
                            }





                            //Berechtigungen RSM (Domailogin)
                            try
                            {
                                //
                                string[] arrges = System.IO.File.ReadAllLines(@"\\G02DEAS01PARE\Public7\CPP_Trust_RSM.csv", Encoding.GetEncoding("ISO-8859-1"));
                                //DataTable acc_tab = new DataTable();
                                String errstrx1 = DateTime.Now.ToLongTimeString() + " anzahl gelesener PartnerDashboard-DebitorAndGroupRSM-Datensätze: " + arrges.Length.ToString() + Environment.NewLine;
                                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstrx1), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstrx1));
                                logstream.Flush();
                                strI += errstrx1;


                                try
                                {
                                    DataTable pb_debitorgroupDomacc = new DataTable();

                                    pb_debitorgroupDomacc.Columns.Add("ResellerGroupID", typeof(string));
                                    pb_debitorgroupDomacc.Columns.Add("ResellerDebitorID", typeof(string));
                                    pb_debitorgroupDomacc.Columns.Add("ResellerDebitorName", typeof(string));
                                    pb_debitorgroupDomacc.Columns.Add("DomAcc", typeof(string));



                                    for (int i = 2; i < arrges.Length; i++)
                                    {

                                        string line = arrges[i];
                                        if (line.Length > 2)
                                        {
                                            string[] linearr = line.Substring(1, line.Length - 2).Split(new string[] { ";" }, StringSplitOptions.None);
                                            //"Account.Id"	"Account.Name"	"Account.Account_ID__c"	"Account.SAP_Id__c"	"Account.BillingStreet"	"Account.BillingCity"	"Account.BillingPostalCode"	"Account.BillingCountry"	"Account.eMail_Address__c"	"Account.Owner.Domain_Account__c"	"Account.Organization__c.Name"	"Account.Sales_Area__c.Name"	"Account.Sales_Region__c.Name"	"Account.Partner__c"	"Account.Group_Name__c"	"Account.SPP_LEVEL_Achieved__c"	"Account.SPP_LEVEL_Committed__c"	"Account.International_Channel_Partner__c"	"Account.Partner_Status__c"	"Account.Channel_Segment__c"	"Account.Local_Channel_Seg__c"	"Account.Account_Segment__c"	"Account.LastModifiedDate"



                                            if (line == "")
                                            {
                                                String errstr1 = "leere Zeile" + Environment.NewLine + Environment.NewLine;
                                                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstr1), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstr1));
                                                logstream.Flush();
                                                //logstr += errstr1;
                                            }

                                            if (linearr.Length != 4)
                                            {

                                                String errstr = linearr.Length + " : " + line + Environment.NewLine + Environment.NewLine;
                                                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstr));
                                                logstream.Flush();
                                                //logstr += errstr;
                                            }
                                            else
                                            {
                                                try
                                                {
                                                    object[] rowarr = new object[4];
                                                    rowarr[0] = linearr[0].ToString().Trim();
                                                    rowarr[1] = "";
                                                    rowarr[2] = linearr[1].ToString().Trim();
                                                    rowarr[3] = linearr[3].ToString().Trim();


                                                    pb_debitorgroupDomacc.Rows.Add(rowarr);

                                                }
                                                catch (Exception se)
                                                {
                                                    String sErr = "x" + se.Message + se.StackTrace + Environment.NewLine;
                                                    logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                                    logstream.Flush();
                                                    lerr = true;

                                                }

                                            }

                                        }

                                        arrges[i] = null;
                                    }
                                    /*   string   errstrx2 = DateTime.Now.ToLongTimeString() + " gelesene PartnerDashboard-DebitorAndGroupRSM-Datensätze: " + pb_debitorgroupDomacc.Rows.Count.ToString() + Environment.NewLine;
                                          logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstrx2), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstrx2));
                                          logstream.Flush();
                                          strI += errstrx2;*/


                                    /***************************************************************************************************************/
                                    List<string> allwithdata = new List<string>();

                                    using (SqlConnection sqlConna = new SqlConnection(ConStr))
                                    {
                                        try
                                        {

                                            string strsql = "select distinct(ResellerGroupID) from  (select resellergroupid from PartnerDashboard_Bonusdata  union select Trustid from PartnerDashboard_Certifications union  select resellergroupid from PartnerDashboard_Revenue) as a";
                                            using (SqlCommand sqlComma = new SqlCommand(strsql, sqlConna))
                                            {
                                                if (sqlConna.State != ConnectionState.Open)
                                                {
                                                    sqlConna.Open();
                                                }
                                                using (SqlDataReader dr = sqlComma.ExecuteReader())
                                                {
                                                    while (dr.Read())
                                                    {
                                                        allwithdata.Add(dr[0].ToString());
                                                    }

                                                }

                                            }

                                        }
                                        catch (Exception err)
                                        {
                                            String sErr = "y2.5" + err.Message + err.StackTrace;
                                            logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                            logstream.Flush();
                                            lerr = true;
                                        }
                                        finally
                                        {
                                            sqlConna.Close();
                                        }
                                    }
                                    Dictionary<string, string> rsmvc = new Dictionary<string, string>();
                                    Dictionary<string, string> rsmirsm = new Dictionary<string, string>();
                                    using (SqlConnection sqlConna = new SqlConnection(ConStrEvt))
                                    {
                                        try
                                        {

                                            string strsql = "select zr.Username as RSMDomAcc, vi.Leader as VCLDomACC from de_event.Vcoverview_zipRSM as zr left join de_event.VCOverview_ZipVC as zv on zr.zip =zv.zip left join de_event.VCOverview_VCInfo as vi on zv.VCShort=vi.Short where zr.type=1 group by zr.Username,vi.Leader";
                                            using (SqlCommand sqlComma = new SqlCommand(strsql, sqlConna))
                                            {
                                                if (sqlConna.State != ConnectionState.Open)
                                                {
                                                    sqlConna.Open();
                                                }
                                                using (SqlDataReader dr = sqlComma.ExecuteReader())
                                                {
                                                    while (dr.Read())
                                                    {
                                                        if (!rsmvc.ContainsKey(dr[0].ToString().Trim().ToUpper()))
                                                        {
                                                            rsmvc.Add(dr[0].ToString().Trim().ToUpper(), dr[1].ToString().Trim().ToUpper());
                                                        }
                                                    }

                                                }

                                            }
                                            strsql = "select zp1.Username, zp2.Username from de_event.VCOVERVIEW_ZipRSM zp1 left join de_event.VCOVERVIEW_ZipRSM zp2 on zp1.Zip=zp2.zip where zp1.Type=1 and zp2.Type=2 and zp1.Zip=(select top(1) zip from de_event.VCOVERVIEW_ZipRSM where username=zp1.Username)";
                                            using (SqlCommand sqlComma = new SqlCommand(strsql, sqlConna))
                                            {
                                                if (sqlConna.State != ConnectionState.Open)
                                                {
                                                    sqlConna.Open();
                                                }
                                                using (SqlDataReader dr = sqlComma.ExecuteReader())
                                                {
                                                    while (dr.Read())
                                                    {
                                                        if (!rsmirsm.ContainsKey(dr[0].ToString().Trim().ToUpper()))
                                                        {
                                                            rsmirsm.Add(dr[0].ToString().Trim().ToUpper(), dr[1].ToString().Trim().ToUpper());
                                                        }
                                                    }

                                                }
                                            }


                                        }
                                        catch (Exception err)
                                        {
                                            String sErr = "y2.6" + err.Message + err.StackTrace;
                                            logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                            logstream.Flush();
                                            lerr = true;
                                        }
                                        finally
                                        {
                                            sqlConna.Close();
                                        }
                                    }




                                    /*************************************************************************************************************/
                                    DataTable pb_debitorgroupDomacc_final = pb_debitorgroupDomacc.Clone();
                                    try
                                    {

                                        List<string> alltrusts = new List<string>();
                                        string showallUser = Properties.Settings.Default.showall;
                                        List<string> additionaluser = new List<string>(showallUser.Split(','));


                                        foreach (DataRow dr in pb_debitorgroupDomacc.Rows)
                                        {
                                            pb_debitorgroupDomacc_final.Rows.Add(dr.ItemArray);

                                            try
                                            {
                                                if (rsmirsm.ContainsKey(dr[3].ToString().Trim()))
                                                {
                                                    String irsmuser = rsmirsm[dr[3].ToString().Trim()];
                                                    if (!String.IsNullOrEmpty(irsmuser))
                                                    {
                                                        DataRow newrow = pb_debitorgroupDomacc_final.NewRow();
                                                        newrow[0] = dr[0].ToString();
                                                        newrow[1] = dr[1].ToString();
                                                        newrow[2] = dr[2].ToString();
                                                        newrow[3] = irsmuser.ToUpper();

                                                        pb_debitorgroupDomacc_final.Rows.Add(newrow);
                                                    }
                                                }
                                                if (allwithdata.Contains(dr[0].ToString()))
                                                {

                                                    if (!alltrusts.Contains(dr[0].ToString()))
                                                    {

                                                        alltrusts.Add(dr[0].ToString());
                                                        try
                                                        {
                                                            foreach (string adduser in additionaluser)
                                                            {
                                                                DataRow newrow = pb_debitorgroupDomacc_final.NewRow();



                                                                newrow[0] = dr[0].ToString();
                                                                newrow[1] = dr[1].ToString();
                                                                newrow[2] = dr[2].ToString();
                                                                newrow[3] = adduser.ToUpper();

                                                                pb_debitorgroupDomacc_final.Rows.Add(newrow);
                                                            }

                                                        }
                                                        catch (Exception err)
                                                        {

                                                            String sErr = "y1x.5" + err.Message + err.StackTrace;
                                                            logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                                            logstream.Flush();
                                                            lerr = true;
                                                        }
                                                        try
                                                        {
                                                            // String sErr ="rsmvc: "+ dr[3].ToString().Trim().ToUpper()+" - "+ rsmvc.ContainsKey(dr[3].ToString().Trim().ToUpper()).ToString();
                                                            // logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                                            // logstream.Flush();
                                                            if (rsmvc.ContainsKey(dr[3].ToString().Trim().ToUpper()))
                                                            {

                                                                DataRow newrow = pb_debitorgroupDomacc_final.NewRow();



                                                                newrow[0] = dr[0].ToString();
                                                                newrow[1] = dr[1].ToString();
                                                                newrow[2] = dr[2].ToString();
                                                                newrow[3] = rsmvc[dr[3].ToString().Trim().ToUpper()].Trim().ToUpper();

                                                                pb_debitorgroupDomacc_final.Rows.Add(newrow);

                                                            }


                                                        }
                                                        catch (Exception err)
                                                        {

                                                            String sErr = "y1x1" + err.Message + err.StackTrace;
                                                            logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                                            logstream.Flush();
                                                            lerr = true;
                                                        }

                                                    }
                                                }
                                            }
                                            catch (Exception err)
                                            {

                                                String sErr = "y1.6" + err.Message + err.StackTrace;
                                                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                                logstream.Flush();
                                                lerr = true;
                                            }
                                        }







                                    }
                                    catch (Exception err)
                                    {
                                        String sErr = "y2.7" + err.Message + err.StackTrace;
                                        logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                        logstream.Flush();
                                        lerr = true;
                                    }
                                    string errstrx2 = DateTime.Now.ToLongTimeString() + " einzufügende PartnerDashboard-DebitorAndGroupRSM-Datensätze: " + pb_debitorgroupDomacc_final.Rows.Count.ToString() + Environment.NewLine;
                                    logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstrx2), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstrx2));
                                    logstream.Flush();
                                    strI += errstrx2;
                                    if (pb_debitorgroupDomacc_final.Rows.Count > 0)
                                    {
                                        SqlConnection sqlConna = new SqlConnection(ConStr);
                                        try
                                        {
                                            SqlCommand sqlComma = new SqlCommand();
                                            sqlComma.Connection = sqlConna;
                                            sqlConna.Open();
                                            string strsql = "delete from PartnerDashboard_DebitorAndGroupRSM ";

                                            sqlComma.CommandText = strsql;
                                            sqlComma.ExecuteNonQuery();


                                            using (SqlBulkCopy sqlBulk = new SqlBulkCopy(ConStr))
                                            {
                                                try
                                                {
                                                    sqlBulk.BatchSize = 10000;
                                                    sqlBulk.NotifyAfter = pb_debitorgroupDomacc_final.Rows.Count;
                                                    sqlBulk.DestinationTableName = "PartnerDashboard_DebitorAndGroupRSM";

                                                    sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ResellerGroupID", "ResellerGroupID"));
                                                    sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ResellerDebitorID", "ResellerDebitorID"));
                                                    sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("ResellerDebitorName", "ResellerDebitorName"));
                                                    sqlBulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping("DomAcc", "DomAcc"));


                                                    sqlBulk.WriteToServer(pb_debitorgroupDomacc_final);
                                                }
                                                catch (Exception e1)
                                                {
                                                    String sErr = "debitorgroupDomacc_final Bulkcopy: " + e1.Message + e1.StackTrace;
                                                    logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                                    logstream.Flush();
                                                    lerr = true;
                                                }
                                            }





                                        }
                                        catch (Exception err)
                                        {
                                            String sErr = "xxx " + err.Message + err.StackTrace;
                                            logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                            logstream.Flush();
                                            lerr = true;
                                        }
                                        finally
                                        {
                                            sqlConna.Close();
                                        }




                                    }
                                }
                                catch (Exception err)
                                {

                                    String sErr = "y3.5" + err.Message + err.StackTrace;
                                    logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                    logstream.Flush();
                                    lerr = true;
                                }
                                finally
                                {

                                }



                            }
                            catch (Exception e1)
                            {
                                String errstr = DateTime.Now.ToLongTimeString() + ": Fehler(2) Import PartnerDashboard-DebitorAndGroupRSM_Domacc : " + e1 + Environment.NewLine;
                                logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstr));
                                logstream.Flush();

                                lerr = true;
                            }









                        }
                        catch (Exception ey)
                        {
                            String errstr = DateTime.Now.ToLongTimeString() + ": Fehler(1) Import PartnerDashboard-DebitorAndGroupRSM : " + ey + Environment.NewLine;
                            logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstr));
                            logstream.Flush();

                            lerr = true;
                        }
                        finally
                        {
                            logstream.Close();
                        }



                        /*************************Impersonated Actions End***********************/

                    }
                    // Releasing the context object stops the impersonation 
                    // Check the identity.
                    // strI += "After closing the context: " + WindowsIdentity.GetCurrent().Name + Environment.NewLine;
                    //logstreamI.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(strI), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(strI));
                    //logstreamI.Flush();
                }
                //logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(str), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(str));
                //logstream.Flush();


            }
            catch (Exception ey)
            {
                strI += DateTime.Now.ToLongTimeString() + ": Fehler import -  Impersonate: " + ey + Environment.NewLine;
                //logstreamI.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstr));
                //logstreamI.Flush();
                //  Console.WriteLine(strI);

            }
            finally
            {
                if (lerr)
                {
                    strI += Environment.NewLine + "Es sind Fehler aufgetreten" + Environment.NewLine;
                }

                strI += Environment.NewLine + "End Import : " + DateTime.Now.ToLongTimeString() + Environment.NewLine;
                SendLog(da.ToLongDateString(), strI);
            }


            SetRSMsToTrust();

            //Console.ReadLine();
            // logstreamI.Close();
        }

        private static void SetTrustsToCSV()
        {
            try
            {
                using (SqlConnection sqlConnection = new SqlConnection(ConStr))
                {
                    sqlConnection.Open();
                    var csv = new StringBuilder();
                   
                    
                    string sqlStmt = @"Select trustid, debitorid, email from PartnerDashboard_RSMDebitor";
                    using (SqlCommand sqlComm = new SqlCommand(sqlStmt, sqlConnection))
                    {
                        using (SqlDataReader dr = sqlComm.ExecuteReader())
                        {
                            try
                            {
                                while (dr.Read())
                                {
                                    csv.AppendLine(dr.GetValue(0).ToString() + ";" + dr.GetValue(1).ToString() + ";" + dr.GetValue(2).ToString() + ";");
                                }
                            }
                            catch (Exception ex1)
                            {
                                Send_failMail(ex1.ToString());
                            }
                        }
                    }

                    File.WriteAllText("test.csv", csv.ToString());
                }

            }
            catch (Exception ex)
            {
                Send_failMail(ex.ToString());
            }
        }

        private static DataTable GetDataFromLocal(string filename)
        {
            DataTable results = new DataTable();
            try
            {
                using (var reader = new StreamReader("C:\\Users\\Christian Werning\\Desktop\\" + filename + ".csv", Encoding.Default, true))
                {
                    bool isFirst = true;
                    while (!reader.EndOfStream)
                    {
                        string line = reader.ReadLine();
                        line = line.Replace("; ", " ");
                        string[] elements = line.Split(';');
                        if (isFirst)
                        {
                            isFirst = false;
                            foreach (string ele in elements)
                            {
                                string formattedElement = ele.Replace("\"", "");
                                results.Columns.Add(formattedElement);
                            }
                        }
                        else
                        {
                            try
                            {
                                for (int i = 0; i < elements.Length; i++)
                                {
                                    elements[i] = elements[i].Replace("\"", "");
                                }

                                results.Rows.Add(elements);
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e.Message);
                            }

                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            return results;
        }

        private static DataRow GetRowByNrAndDebitorId(DataRowCollection rows, string debitorID, string trustID)
        {
            foreach (DataRow row in rows)
            {
                if (row["DebitorID"].ToString() == debitorID && row["TrustID"].ToString() == trustID)
                {
                    return row;
                }
            }
            return null;
        }

        private static DataTable GetDataFromExchange(string sf)
        {
            DataTable ergtable = new DataTable();
            try
            {
                ExchangeService _mailService = new ExchangeService(ExchangeVersion.Exchange2007_SP1);
                //_mailService.Credentials = CredentialCache.DefaultNetworkCredentials;
                _mailService.Credentials = new System.Net.NetworkCredential(@"cpp-de-nl-p7", @"eywAGC695(&*", @"g02");
                _mailService.Url = new Uri("https://webmail.emeia.fujitsu.local/ews/exchange.asmx");
                Folder inbox = Folder.Bind(_mailService, WellKnownFolderName.Inbox);
                FolderId fid = null;

                foreach (Folder folder in inbox.FindFolders(new FolderView(100)))
                {
                    if (folder.DisplayName.Equals("certification report"))
                    {
                        fid = folder.Id;
                    }
                }


                List<SearchFilter> searchFilterCollection = new List<SearchFilter>();
                searchFilterCollection.Add(new SearchFilter.ContainsSubstring(ItemSchema.Subject, sf));

                SearchFilter searchFilter = new SearchFilter.SearchFilterCollection(LogicalOperator.And, searchFilterCollection.ToArray());

                ItemView view = new ItemView(1);

                view.PropertySet = new PropertySet(BasePropertySet.IdOnly, ItemSchema.Subject, ItemSchema.DateTimeReceived);
                view.OrderBy.Add(ItemSchema.DateTimeReceived, SortDirection.Descending);
                FindItemsResults<Item> findResults = _mailService.FindItems(WellKnownFolderName.Inbox, searchFilter, view);


                foreach (Item myItem in findResults.Items)
                {
                    Console.WriteLine("Items found for: " + sf);
                    if (myItem is EmailMessage)
                    {
                        EmailMessage myEmail = EmailMessage.Bind(_mailService, myItem.Id, new PropertySet(ItemSchema.Attachments));

                        foreach (Attachment attachment in myEmail.Attachments)
                        {

                            if (attachment is FileAttachment)
                            {
                                FileAttachment fileAttachment = attachment as FileAttachment;
                                fileAttachment.Load("C:\\tasks\\PartnerDashboardImport\\input\\" + fileAttachment.Name);
                                // System.IO.Compression.ZipFile.ExtractToDirectory("C:\\tasks\\PartnerDashboardImport\\input\\" + fileAttachment.Name, "C:\\tasks\\PartnerDashboardImport\\input\\");
                                string csvname = fileAttachment.Name.Replace(".zip", ".csv");

                                if (File.Exists("C:\\tasks\\PartnerDashboardImport\\input\\" + csvname))
                                {
                                    File.Delete("C:\\tasks\\PartnerDashboardImport\\input\\" + csvname);
                                }

                                System.IO.Compression.ZipFile.ExtractToDirectory("C:\\tasks\\PartnerDashboardImport\\input\\" + fileAttachment.Name, "C:\\tasks\\PartnerDashboardImport\\input\\");

                                if (File.Exists("C:\\tasks\\PartnerDashboardImport\\input\\" + csvname))
                                {
                                    //string[] arrges = System.IO.File.ReadAllLines("C:\\tasks\\PartnerDashboardImport\\input\\" + csvname, Encoding.UTF8);
                                    try
                                    {
                                        try
                                        {
                                            using (var reader = new StreamReader("C:\\tasks\\PartnerDashboardImport\\input\\" + csvname, Encoding.Default, true))
                                            {
                                                bool isFirst = true;
                                                while (!reader.EndOfStream)
                                                {
                                                    string line = reader.ReadLine();
                                                    line = line.Replace("; ", " ");
                                                    string[] elements = line.Split(';');
                                                    if (isFirst)
                                                    {
                                                        isFirst = false;
                                                        foreach (string ele in elements)
                                                        {
                                                            string formattedElement = ele.Replace("\"", "");
                                                            ergtable.Columns.Add(formattedElement);
                                                        }
                                                    }
                                                    else
                                                    {
                                                        try
                                                        {
                                                            for (int j = 0; j < elements.Length; j++)
                                                            {
                                                                elements[j] = elements[j].Replace("\"", "");
                                                            }

                                                            ergtable.Rows.Add(elements);
                                                        }
                                                        catch (Exception e)
                                                        {
                                                            Console.WriteLine(e.Message);
                                                        }

                                                    }
                                                }
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            Console.WriteLine(e.Message);
                                        }

                                    }
                                    catch (Exception err)
                                    {

                                        String sErr = "y" + err.Message + err.StackTrace;
                                        logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(sErr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(sErr));
                                        logstream.Flush();
                                        lerr = true;
                                    }
                                }
                            }
                        }
                        if (fid != null)
                        {
                            myItem.Move(fid);

                        }
                    }


                }







            }
            catch (Exception e1)
            {
                String errstr = DateTime.Now.ToLongTimeString() + ": Fehler in GetDataFromExchange: " + e1 + Environment.NewLine;
                if (logstream != null && logstream.CanWrite)
                {
                    logstream.Write(Encoding.GetEncoding("iso-8859-1").GetBytes(errstr), 0, Encoding.GetEncoding("iso-8859-1").GetByteCount(errstr));
                    logstream.Flush();
                }

                lerr = true;
            }
            return ergtable;
        }


        public static void SetRSMsToTrust()
        {

            List<string[]> trusts = new List<string[]>();

            try
            {
                using (SqlConnection sqlConnection = new SqlConnection(ConStr))
                {
                    sqlConnection.Open();

                    string sqlStmtDelete = @"DELETE FROM PartnerDashboard_RSMDebitor";
                    using (SqlCommand sqlComm = new SqlCommand(sqlStmtDelete, sqlConnection))
                    {
                        sqlComm.ExecuteReader();
                    }

                    sqlConnection.Close();
                    sqlConnection.Open();

                    string sqlStmt = @"Select DISTINCT ResellerGroupID, ResellerDebitorNr from PartnerDashboard_Revenue
                                        WHERE ResellerDebitorNr != ''";
                    using (SqlCommand sqlComm = new SqlCommand(sqlStmt, sqlConnection))
                    {
                        using (SqlDataReader dr = sqlComm.ExecuteReader())
                        {
                            try
                            {
                                while (dr.Read())
                                {
                                    string trustId = dr.GetValue(0).ToString();
                                    string debitorId = dr.GetValue(1).ToString();
                                    string[] returnArr = GetAccountOwnerBySF(debitorId);
                                    trusts.Add(new string[] {trustId, debitorId, returnArr[0], returnArr[1], returnArr[2]});
                                }
                            }
                            catch (Exception ex1)
                            {
                                Send_failMail(ex1.ToString());
                            }
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                Send_failMail(ex.ToString());
            }

            foreach(string[] trust in trusts)
            {
                try
                {
                    using (SqlConnection sqlConnection = new SqlConnection(ConStr))
                    {
                        sqlConnection.Open();
                        using (SqlCommand cmd = sqlConnection.CreateCommand())
                        {
                            cmd.CommandText = @"INSERT INTO PartnerDashboard_RSMDebitor VALUES (@trustid, @debitorid, @email, @salesregion, @fullname)";
                            using (SqlDataAdapter adpt = new SqlDataAdapter(cmd))
                            {

                                cmd.Parameters.Add("@trustid", SqlDbType.VarChar, 255);
                                cmd.Parameters.Add("@debitorid", SqlDbType.VarChar, 255);
                                cmd.Parameters.Add("@email", SqlDbType.VarChar, 255);
                                cmd.Parameters.Add("@salesregion", SqlDbType.VarChar, 255);
                                cmd.Parameters.Add("@fullname", SqlDbType.VarChar, 255);
                                cmd.Parameters[0].Value = trust[0];
                                cmd.Parameters[1].Value = trust[1];
                                cmd.Parameters[2].Value = trust[2];
                                cmd.Parameters[3].Value = trust[3];
                                cmd.Parameters[4].Value = trust[4];

                                cmd.ExecuteReader();
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Send_failMail(ex.ToString());
                }
            }
        }


        public static string[] GetAccountOwnerBySF(string debitorId, int counter = 0)
        {
            string accountOwner = "";
            string salesRegion = "";
            string fullname = "";
            string[] returnArr = new string[3];
            PrepareSF();
            QueryResult qr = null;
            string stmt = "SELECT Account_Owner_Email__c, Sales_Region__r.Name, Owner_Full_Name__c FROM Account WHERE Account.Account_ID__c = '" + debitorId+ "' OR SAP_Id__c = '" + debitorId+"'";

            try
            {
                qr = SfdcBinding.query(stmt);
            }
            catch (Exception)
            {
                if (counter == 0)
                {
                    SfdcBinding = null;
                    return GetAccountOwnerBySF(debitorId, 1);
                }
            }
            

            if (qr.size > 0)
            {


                sObject[] records = qr.records;

                for (int i = 0; i < records.Length; ++i)
                {
                    sObject con = qr.records[i];
                    if (con.Any[0].InnerText != "")
                    {
                        accountOwner = con.Any[0].InnerText;
                    }
                    if (con.Any[1].InnerText != "")
                    {
                        salesRegion = con.Any[1].InnerText;
                    }
                    if (con.Any[2].InnerText != "")
                    {
                        fullname = con.Any[2].InnerText;
                    }
                }

            }

            returnArr[0] = accountOwner;
            returnArr[1] = salesRegion.Replace("Sales_Region__c", "");
            returnArr[2] = fullname;
            return returnArr;
        }

        public static List<string> DescribeObject(string objectName)
        {
            PrepareSF();
            DescribeSObjectResult qr = SfdcBinding.describeSObject(objectName);
            List<string> fieldList = new List<string>();

            foreach (Field sField in qr.fields)
            {
                fieldList.Add(sField.name);
            }
            return fieldList;

        }

        public static void SendLog(string datum, string erg)
        {

            using (MailMessage message = new MailMessage())
            {
                using (SmtpClient client = new SmtpClient("mail.fsc.net"))
                {

                    message.From = new MailAddress(Properties.Settings.Default.logmailfrom);
                    message.To.Add(new MailAddress(Properties.Settings.Default.logmailto));
                    if (!String.IsNullOrEmpty(Properties.Settings.Default.logmailcc))
                    {
                        string[] allmailccs = Properties.Settings.Default.logmailcc.Split(',');
                        foreach (string email in allmailccs)
                        {
                            if (!String.IsNullOrEmpty(email.Trim()))
                            {
                                message.CC.Add(new MailAddress(email.Trim()));
                            }
                        }
                    }




                    message.Subject = "Log PartnerDashboard-Import: " + datum;
                    if (lerr)
                    {
                        message.Subject += " - Es sind Fehler aufgetreten";
                    }
                    message.Body = erg;


                    try
                    {
                        client.Send(message);

                    }
                    catch
                    {
                        // Console.WriteLine(ex.ToString());
                    }
                }
            }

        }
        public static void Send_failMail(string erg)
        {

            using (MailMessage message = new MailMessage())
            {
                using (SmtpClient client = new SmtpClient("mail.fsc.net"))
                {

                    message.From = new MailAddress(Properties.Settings.Default.logmailfrom);
                    message.To.Add(new MailAddress(Properties.Settings.Default.logmailto));
                    if (!String.IsNullOrEmpty(Properties.Settings.Default.logmailcc))
                    {
                        string[] allmailccs = Properties.Settings.Default.logmailcc.Split(',');
                        foreach (string email in allmailccs)
                        {
                            if (!String.IsNullOrEmpty(email.Trim()))
                            {
                                message.CC.Add(new MailAddress(email.Trim()));
                            }
                        }
                    }

                    message.Subject = "Fehler PartnerDashboard-Import: Get SFService " + DateTime.Now.ToShortDateString() + " " + DateTime.Now.ToShortTimeString();
                    message.Body = erg;


                    try
                    {
                        client.Send(message);

                    }
                    catch
                    {
                        // Console.WriteLine(ex.ToString());
                    }
                }
            }


        }


        private static void PrepareSF()
        {
            ServicePointManager.Expect100Continue = true;
            //  ServicePointManager.SecurityProtocol = (SecurityProtocolType)3072;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Ssl3 | SecurityProtocolType.Tls | (SecurityProtocolType)3072 | (SecurityProtocolType)768;

            ServicePointManager.DefaultConnectionLimit = 9999;
            if (SfdcBinding == null)
            {
                GetSFService(false);
            }
        }


        private static void GetSFService(bool debug)
        {

            // SforceService sfdcBinding = null;
            try
            {
                //string userName = "cpp@public7.de.test";
                string userName = "cpp@public7.de";
                string password = "RFU4G0Y1sA-=";
                string securityToken = "RhBTbBODExQbriL7t50a4fJPa";

                LoginResult currentLoginResult = null;
                SfdcBinding = new SforceService();


                // abgproxya.abg.fsc.net:82  abgproxya.abg.fsc.net:81



                if (!debug)
                {
                    // abgproxya.abg.fsc.net:82  abgproxya.abg.fsc.net:81
                    SfdcBinding.Proxy = new WebProxy("http://G02DEPXABGB000.g02.fujitsu.local:82", false);
                }

                try
                {
                    currentLoginResult = SfdcBinding.login(userName, password + securityToken);

                }
                catch (Exception)
                {
                    // This is likley to be caused by bad username or password
                    SfdcBinding = new SforceService();
                    // throw (ex);
                    // Send_failMail("Versuch1 http://G02DEPXABGB000.g02.fujitsu.local:82: " + ex.ToString());
                    if (!debug)
                    {
                        SfdcBinding.Proxy = new WebProxy("http://G02DEPXMCHQ000.g02.fujitsu.local:82", false);
                    }

                    try
                    {
                        currentLoginResult = SfdcBinding.login(userName, password + securityToken);

                    }
                    catch (Exception)
                    {

                        SfdcBinding = new SforceService();
                        // throw (ex);
                        // Send_failMail("Versuch1 http://G02DEPXABGB000.g02.fujitsu.local:82: " + ex.ToString());
                        if (!debug)
                        {
                            SfdcBinding.Proxy = new WebProxy("http://abgproxya.abg.fsc.net:82", false);
                        }

                        try
                        {
                            currentLoginResult = SfdcBinding.login(userName, password + securityToken);

                        }
                        catch (Exception)
                        {
                            // This is likley to be caused by bad username or password
                            SfdcBinding = new SforceService();
                            SfdcBinding = null;

                        }



                    }
                }

                //Change the binding to the new endpoint
                SfdcBinding.Url = currentLoginResult.serverUrl;

                //Create a new session header object and set the session id to that returned by the login
                SfdcBinding.SessionHeaderValue = new SessionHeader
                {
                    sessionId = currentLoginResult.sessionId
                };
            }
            catch (Exception)
            {

            }
        }

    }

    public sealed class SafeTokenHandle : SafeHandleZeroOrMinusOneIsInvalid
    {
        private SafeTokenHandle()
            : base(true)
        {
        }

        [DllImport("kernel32.dll")]
        [ReliabilityContract(Consistency.WillNotCorruptState, Cer.Success)]
        [SuppressUnmanagedCodeSecurity]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool CloseHandle(IntPtr handle);

        protected override bool ReleaseHandle()
        {
            return CloseHandle(handle);
        }
    }




}

