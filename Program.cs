// Target framework for the code: .NET 8.0
// Standard Motor Products Inc. (SMP) Forex Import Daily Job Project
using Microsoft.Extensions.Configuration;
using System.Data;
using System.Data.OleDb; // Use OLE DB for IBM DB2 for i
using System.Globalization;
using System.Net;
using System.Net.Mail;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
    .AddEnvironmentVariables()
#if DEBUG
    .AddUserSecrets(typeof(Program).Assembly, optional: true)
#endif
    .Build();

string Db2ConnectionString =
    configuration.GetConnectionString("Db2")
    ?? Environment.GetEnvironmentVariable("ConnectionStrings__Db2")
    ?? throw new InvalidOperationException("ConnectionStrings:Db2 is missing.");

// urls array
string[] urls = configuration.GetSection("Forex:Urls").Get<string[]>()
    ?? throw new InvalidOperationException("Forex:Urls is missing or empty.");

var http = new HttpClient();
var jsonOptions = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
string JDELibrary = configuration.GetValue<string>("JDELibrary") ?? "JDFDTAI";

string ToEmail = configuration.GetValue<string>("ToEmail").ToString();
string NoReply = configuration.GetValue<string>("NoReply").ToString();
string HostName = configuration.GetValue<string>("HostName").ToString();

try
{
    //int test = 10;
    //int t = test / 0;

    var fetchTasks = urls.Select(async url =>
    {
        using var resp = await http.GetAsync(url);
        resp.EnsureSuccessStatusCode();
        var stream = await resp.Content.ReadAsStreamAsync();
        var data = await JsonSerializer.DeserializeAsync<ExchangeRatesResponse>(stream, jsonOptions)
                   ?? throw new InvalidOperationException("Empty JSON payload.");
        data.SourceUrl = url;
        return data;
    }).ToArray();

    var results = await Task.WhenAll(fetchTasks);
    var rows = results.SelectMany(r =>
        r.Rates.Select(kvp => new ExchangeRateRow
        {
            BaseCurrency = r.Base,
            QuoteCurrency = kvp.Key,
            Rate = kvp.Value,
            Date = r.Date,
            FetchedAtUtc = DateTime.UtcNow,
            SourceUrl = r.SourceUrl
        }))
        .ToList();

    if (rows.Count == 0)
    {
        //Console.WriteLine("No data to insert.");
        return;
    }

    string CXCRDC = string.Empty;
    string CXCRRD = string.Empty;
    string PXCRRD = string.Empty;
    string BASECUR = string.Empty;
    string efDate = string.Empty;
    int rowCount = 0;

    foreach (var row in rows)
    {
        CXCRDC = NormalizeCurrency(row.QuoteCurrency);
        BASECUR = NormalizeCurrency(row.BaseCurrency);

        Char szero = '0';
        CXCRRD = row.Rate.ToString(); 
        string szerostring = "0";
        if (CXCRRD.Length == 1)
        {
            CXCRRD = CXCRRD + "." + szerostring.ToString().PadRight(7, szero);
        }
        else
        {
            if (CXCRRD.IndexOf(".") == -1)
            {
                CXCRRD = CXCRRD + "." + szerostring.ToString().PadRight(7, szero);
            } else {
                CXCRRD = CXCRRD.ToString().PadRight(7, szero);
            }
        }

        PXCRRD = (1 / float.Parse(CXCRRD)).ToString();
        efDate = row.FetchedAtUtc.ToString();
        efDate = ToJdeJulian(DateTime.Parse(efDate)).ToString();

        int jdeDate = ToJdeJulian(row.FetchedAtUtc);
        bool exists = RecordExists(JDELibrary, "F550015", BASECUR, CXCRDC, jdeDate);
        if (!exists)
        {
            // insert…
            bool ret = SaveExchangeRate(CXCRDC, CXCRRD, PXCRRD, efDate, BASECUR);
            if (ret)
            {
                rowCount = rowCount + 1;
            }
        }        
    }

    // Console.WriteLine($"Inserted/Updated {rows.Count} rows.");

    //Call JDE Procedure to process the exchange rates
    CallJdeProc();

    //Sent Success Email
    SendEmail(
        to: ToEmail, // "dexter.faraira@smpcorp.com",
        subject: "SMP Forex Daily Job Success",
        htmlBody: "<h2>SMP Forex Job</h2><p>Hi Team,<br/><br/>SMP Forex Daily Job run Successfully.<br/><br/>Total " + rowCount.ToString() + " exchange rates processed." + "<br/><br/>Thank you<br/></p>"
    );

}
catch (HttpRequestException ex)
{
    //Console.Error.WriteLine($"HTTP error: {ex.Message}");
    ErrorEmail(ex.Message.ToString());        

}
catch (JsonException ex)
{
    //Console.Error.WriteLine($"JSON parse error: {ex.Message}");
    ErrorEmail(ex.Message.ToString());
}
catch (OleDbException ex)
{
    //Console.Error.WriteLine($"DB2 OLE DB error: {ex.Message}");
    ErrorEmail(ex.Message.ToString());
}
catch (Exception ex)
{
    //Console.Error.WriteLine($"Unexpected error: {ex}");
    ErrorEmail(ex.Message.ToString());
}
void ErrorEmail(string Err)
{
    SendEmail(
        to: ToEmail, //"dexter.faraira@smpcorp.com",
        subject: "SMP Forex Job Failed",
        htmlBody: "<h2>SMP Forex Job</h2><p>Hi Team,<br/><br/>SMP Forex Daily Job failed.<br/><br/></p>" +
                         $"<p>Error details: {Err}</p>" +
                  "<p>Please investigate the issue.<br/><br/>Thank you<br/> </p>"
    );
}

string NormalizeCurrency(string? currency)
{
    if (string.IsNullOrWhiteSpace(currency))
        throw new ArgumentException("Currency is required.", nameof(currency));

    // Normalize input
    string cur = currency.Trim().ToUpperInvariant();

    // Basic validation: A–Z letters, length 3
    if (cur.Length != 3 || !cur.All(ch => ch is >= 'A' and <= 'Z'))
        throw new ArgumentException($"Invalid currency format: {currency}", nameof(currency));

    // Map special cases
    return cur switch
    {
        "CAD" => "CDN",
        "MXN" => "MXP",
        _ => cur
    };
}
bool CallJdeProc()
{
    bool retFlag = false;
    
    string connstr = Db2ConnectionString;
    using OleDbConnection SMPLEWConnection = new OleDbConnection(connstr);

    try
    {
        string SQL = configuration.GetValue<string>("JDEPROC") ?? "CALL JDFCST.P550015";

        OleDbCommand OleCommand = new OleDbCommand(SQL, SMPLEWConnection);
        OleCommand.CommandType = CommandType.Text;

        OleCommand.Connection.Open();
        int ret = OleCommand.ExecuteNonQuery();
        SMPLEWConnection.Close();
        retFlag = true;
    }
    catch (Exception ex)
    {
        //FailComponent(ex.Message.ToString());
        ErrorEmail(ex.Message.ToString());
    }
    return retFlag;
}

void SendEmail(string to, string subject, string htmlBody)
{
    // Configure sender and SMTP
    var fromAddress = NoReply;// "no-reply@smpcorp.com"; // change if needed
    var smtpHost = HostName;// "mail.smpcorp.com";
    var smtpPort = 25; // common ports: 25, 587, 465
    var enableSsl = false; // set according to server policy
    var smtpUser = string.Empty;// "no-reply@smpcorp.com"; // or service account
    var smtpPass = string.Empty; //"YOUR_SMTP_PASSWORD";   // secure via env/user-secrets

    using var message = new MailMessage(fromAddress, to)
    {
        Subject = subject,
        Body = htmlBody,
        IsBodyHtml = true
    };

    using var client = new SmtpClient(smtpHost, smtpPort)
    {
        EnableSsl = enableSsl,
        Credentials = new NetworkCredential(smtpUser, smtpPass),
        DeliveryMethod = SmtpDeliveryMethod.Network,
        UseDefaultCredentials = false
    };

    client.Send(message);
}

bool SaveExchangeRate(string CXCRDC, string CXCRRD, string PXCRRD, string efDate, string baseCur)
{
    bool retFlag = false;
    string connstr = Db2ConnectionString;
    using OleDbConnection SMPLEWConnection = new OleDbConnection(connstr);
    try
    {
        string sTime = string.Format("{0:d/M/yyyy HH:mm:ss}", DateTime.Now);
        sTime = sTime.Replace(":", "").Substring(10).Trim();

        string SQL = "INSERT INTO " + JDELibrary + ".F550015" + " ( PXCRCD,  PXEFT,   PXCRR,  PXAN8, PXCDEC, PXCRDC, PXCRRD,PXUSER,PXUPMJ, PXPID,PXJOBN,PXTDAY ) " +
        "Values  ('" + baseCur + "', '" + efDate + "'," + PXCRRD + ",  0,  0, '" + CXCRDC.Trim() + "'," + CXCRRD + ",'WINJOBUSER','" + efDate + "','FOREXEXCH','FOREXAPI'," + sTime.ToString() + ") ";
        SQL = SQL + ",  ('" + CXCRDC.Trim() + "', '" + efDate + "'," + CXCRRD + ",  0,  0, '" + baseCur + "'," + PXCRRD + ",'WINJOBUSER','" + efDate + "','FOREXEXCH','FOREXAPI'," + sTime.ToString() + ") ";

        OleDbCommand myCommand = new OleDbCommand(SQL, SMPLEWConnection);
        myCommand.CommandType = CommandType.Text;

        myCommand.Connection.Open();
        int ret = myCommand.ExecuteNonQuery();
        SMPLEWConnection.Close();
        retFlag = true;                
    }
    catch (Exception ex)
    {
        //FailComponent(ex.Message.ToString());
        ErrorEmail(ex.Message.ToString());
    }
    return retFlag;
}

static int ToJdeJulian(DateTime date)
{
    date = date.Date;
    return (date.Year - 1900) * 1000 + date.DayOfYear; // CYY*1000 + DDD
}

bool RecordExists(string library, string table, string baseCcy, string quoteCcy, int jdeJulianDate, OleDbConnection? externalConn = null)
{
    // Use an external open connection if provided; otherwise create one.
    using var conn = externalConn ?? new OleDbConnection(Db2ConnectionString);
    if (conn.State != ConnectionState.Open)
        conn.Open();

    // Adjust the predicate to your actual key columns. Example uses PXCRCD (quote), PXCRDC (base), PXUPMJ (JDE Julian date)
    // FETCH FIRST 1 ROW ONLY avoids scanning/counting entire table.
    string sql = $@"
        SELECT 1
          FROM {library}.{table}
         WHERE PXCRCD = ?
           AND PXCRDC = ?
           AND PXUPMJ = ?
        FETCH FIRST 1 ROW ONLY";

    using var cmd = new OleDbCommand(sql, conn) { CommandType = CommandType.Text };
    // Order of parameters matters with OLE DB ('?' placeholders are positional)
    cmd.Parameters.Add(new OleDbParameter { Value = quoteCcy });   // PXCRCD
    cmd.Parameters.Add(new OleDbParameter { Value = baseCcy });    // PXCRDC
    cmd.Parameters.Add(new OleDbParameter { Value = jdeJulianDate }); // PXUPMJ (int)

    using var reader = cmd.ExecuteReader();
    return reader.Read(); // true if at least one row
}

public sealed class ExchangeRatesResponse
{
    [JsonPropertyName("success")] public bool? Success { get; set; }
    [JsonPropertyName("base")] public string? Base { get; set; }
    [JsonPropertyName("date")] public string? DateRaw { get; set; }
    [JsonPropertyName("rates")] public Dictionary<string, decimal> Rates { get; set; } = new();
    [JsonIgnore] public string? SourceUrl { get; set; }
    [JsonIgnore]
    public DateTime? Date =>
        DateTime.TryParse(DateRaw, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dt)
            ? dt.Date : null;
}

public sealed class ExchangeRateRow
{
    public string? BaseCurrency { get; set; }
    public string? QuoteCurrency { get; set; }
    public decimal Rate { get; set; }
    public DateTime? Date { get; set; }
    public DateTime FetchedAtUtc { get; set; }
    public string? SourceUrl { get; set; }
}


