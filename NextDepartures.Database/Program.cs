using System;
using System.Data;
using System.Data.Common;
using GTFS;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using NextDepartures.Database.Extensions;

GTFSReader<GTFSFeed> reader = new();
var feed = reader.Read(path: "Data/GTFS.zip");

var azureSql = "Server=tcp:yourserver.database.windows.net,1433;" +
               "Initial Catalog=NextDepartures;" +
               "User ID=myuser;" +
               "Password=mypassword;" +
               "Encrypt=True;" +
               "TrustServerCertificate=False;" +
               "Connection Timeout=30;";

await using DbConnection connection = string.IsNullOrWhiteSpace(azureSql)
    ? new SqliteConnection("Data Source=Data/feed.db;")
    : new SqlConnection(azureSql);
await connection.OpenAsync();

DbCommand command = string.IsNullOrWhiteSpace(azureSql)
    ? new SqliteCommand()
    : new SqlCommand();
command.CommandTimeout = 0;
command.CommandType = CommandType.Text;
command.Connection = connection;

command.CommandText = "INSERT INTO GTFS_AGENCY (" +
                            "AgencyId, " +
                            "AgencyName, " +
                            "AgencyUrl, " +
                            "AgencyTimezone, " +
                            "AgencyLang, " +
                            "AgencyPhone, " +
                            "AgencyFareUrl, " +
                            "AgencyEmail) " +
                      "VALUES (" +
                            "@agencyId, " +
                            "@agencyName, " +
                            "@agencyUrl, " +
                            "@agencyTimezone, " +
                            "@agencyLang, " +
                            "@agencyPhone, " +
                            "@agencyFareUrl, " +
                            "@agencyEmail)";

command.Parameters.Clear();

using (var transaction = connection.BeginTransaction())
{
    command.Transaction = transaction;

    foreach (var a in feed.Agencies)
    {
        command.Parameters.Clear();
    
    command.AddWithValue(
        parameterName: "@agencyId",
        value: a.Id is not null
            ? a.Id.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@agencyName",
        value: a.Name.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@agencyUrl",
        value: a.URL.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@agencyTimezone",
        value: a.Timezone.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@agencyLang",
        value: a.LanguageCode is not null
            ? a.LanguageCode.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@agencyPhone",
        value: a.Phone is not null
            ? a.Phone.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@agencyFareUrl",
        value: a.FareURL is not null
            ? a.FareURL.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@agencyEmail",
        value: a.Email is not null
            ? a.Email.TrimDoubleQuotes()
            : DBNull.Value);

        await command.ExecuteNonQueryAsync();
    }

    transaction.Commit();
    
}

command.Transaction = null;
command.Parameters.Clear();

command.CommandText = "INSERT INTO GTFS_CALENDAR (" +
                            "ServiceId, " +
                            "Monday, " +
                            "Tuesday, " +
                            "Wednesday, " +
                            "Thursday, " +
                            "Friday, " +
                            "Saturday, " +
                            "Sunday, " +
                            "StartDate, " +
                            "EndDate) " +
                      "VALUES (" +
                            "@serviceId, " +
                            "@monday, " +
                            "@tuesday, " +
                            "@wednesday, " +
                            "@thursday, " +
                            "@friday, " +
                            "@saturday, " +
                            "@sunday, " +
                            "@startDate, " +
                            "@endDate)";

command.Parameters.Clear();

using (var transaction = connection.BeginTransaction())
{
    command.Transaction = transaction;

    foreach (var c in feed.Calendars)
    {
        command.Parameters.Clear();
    
    command.AddWithValue(
        parameterName: "@serviceId",
        value: c.ServiceId.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@monday",
        value: c.Monday);
    
    command.AddWithValue(
        parameterName: "@tuesday",
        value: c.Tuesday);
    
    command.AddWithValue(
        parameterName: "@wednesday",
        value: c.Wednesday);
    
    command.AddWithValue(
        parameterName: "@thursday",
        value: c.Thursday);
    
    command.AddWithValue(
        parameterName: "@friday",
        value: c.Friday);
    
    command.AddWithValue(
        parameterName: "@saturday",
        value: c.Saturday);
    
    command.AddWithValue(
        parameterName: "@sunday",
        value: c.Sunday);
    
    command.AddWithValue(
        parameterName: "@startDate",
        value: c.StartDate);
    
    command.AddWithValue(
        parameterName: "@endDate",
        value: c.EndDate);

        await command.ExecuteNonQueryAsync();
    }

    transaction.Commit();
    
}

command.Transaction = null;
command.Parameters.Clear();

command.CommandText = "INSERT INTO GTFS_CALENDAR_DATES (" +
                            "ServiceId, " +
                            "ExceptionDate, " +
                            "ExceptionType) " +
                      "VALUES (" +
                            "@serviceId, " +
                            "@exceptionDate, " +
                            "@exceptionType)";

command.Parameters.Clear();

using (var transaction = connection.BeginTransaction())
{
    command.Transaction = transaction;

    foreach (var d in feed.CalendarDates)
    {
        command.Parameters.Clear();
    
    command.AddWithValue(
        parameterName: "@serviceId",
        value: d.ServiceId.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@exceptionDate",
        value: d.Date);
    
    command.AddWithValue(
        parameterName: "@exceptionType",
        value: d.ExceptionType);

        await command.ExecuteNonQueryAsync();
    }

    transaction.Commit();
    
}

command.Transaction = null;
command.Parameters.Clear();

command.CommandText = "INSERT INTO GTFS_FARE_ATTRIBUTES (" +
                            "FareId, " +
                            "Price, " +
                            "CurrencyType, " +
                            "PaymentMethod, " +
                            "Transfers, " +
                            "AgencyId, " +
                            "TransferDuration) " +
                      "VALUES (" +
                            "@fareId, " +
                            "@price, " +
                            "@currencyType, " +
                            "@paymentMethod, " +
                            "@transfers, " +
                            "@agencyId, " +
                            "@transferDuration)";

command.Parameters.Clear();

using (var transaction = connection.BeginTransaction())
{
    command.Transaction = transaction;

    foreach (var f in feed.FareAttributes)
    {
        command.Parameters.Clear();
    
    command.AddWithValue(
        parameterName: "@fareId",
        value: f.FareId.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@price",
        value: f.Price.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@currencyType",
        value: f.CurrencyType.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@paymentMethod",
        value: f.PaymentMethod);
    
    command.AddWithValue(
        parameterName: "@transfers",
        value: f.Transfers.ToString() != string.Empty
            ? f.Transfers
            : string.Empty);
    
    command.AddWithValue(
        parameterName: "@agencyId",
        value: f.AgencyId is not null
            ? f.AgencyId.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@transferDuration",
        value: f.TransferDuration is not null
            ? f.TransferDuration.TrimDoubleQuotes()
            : DBNull.Value);

        await command.ExecuteNonQueryAsync();
    }

    transaction.Commit();
    
}

command.Transaction = null;
command.Parameters.Clear();

command.CommandText = "INSERT INTO GTFS_FARE_RULES (" +
                            "FareId, " +
                            "RouteId, " +
                            "OriginId, " +
                            "DestinationId, " +
                            "ContainsId) " +
                      "VALUES (" +
                            "@fareId, " +
                            "@routeId, " +
                            "@originId, " +
                            "@destinationId, " +
                            "@containsId)";

command.Parameters.Clear();

using (var transaction = connection.BeginTransaction())
{
    command.Transaction = transaction;

    foreach (var f in feed.FareRules)
    {
        command.Parameters.Clear();
    
    command.AddWithValue(
        parameterName: "@fareId",
        value: f.FareId.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@routeId",
        value: f.RouteId is not null
            ? f.RouteId.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@originId",
        value: f.OriginId is not null
            ? f.OriginId.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@destinationId",
        value: f.DestinationId is not null
            ? f.DestinationId.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@containsId",
        value: f.ContainsId is not null
            ? f.ContainsId.TrimDoubleQuotes()
            : DBNull.Value);

        await command.ExecuteNonQueryAsync();
    }

    transaction.Commit();
    
}

command.Transaction = null;
command.Parameters.Clear();

command.CommandText = "INSERT INTO GTFS_FREQUENCIES (" +
                            "TripId, " +
                            "StartTime, " +
                            "EndTime, " +
                            "HeadwaySecs, " +
                            "ExactTimes) " +
                      "VALUES (" +
                            "@tripId, " +
                            "@startTime, " +
                            "@endTime, " +
                            "@headwaySecs, " +
                            "@exactTimes)";

command.Parameters.Clear();

using (var transaction = connection.BeginTransaction())
{
    command.Transaction = transaction;

    foreach (var f in feed.Frequencies)
    {
        command.Parameters.Clear();
    
    command.AddWithValue(
        parameterName: "@tripId",
        value: f.TripId.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@startTime",
        value: f.StartTime.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@endTime",
        value: f.EndTime.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@headwaySecs",
        value: f.HeadwaySecs.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@exactTimes",
        value: f.ExactTimes is not null
            ? f.ExactTimes.ToString() != string.Empty
                ? f.ExactTimes
                : string.Empty
            : DBNull.Value);

        await command.ExecuteNonQueryAsync();
    }

    transaction.Commit();
    
}

command.Transaction = null;
command.Parameters.Clear();

command.CommandText = "INSERT INTO GTFS_LEVELS (" +
                            "LevelId, " +
                            "LevelIndex, " +
                            "LevelName) " +
                      "VALUES (" +
                            "@levelId, " +
                            "@levelIndex, " +
                            "@levelName)";

command.Parameters.Clear();

using (var transaction = connection.BeginTransaction())
{
    command.Transaction = transaction;

    foreach (var l in feed.Levels)
    {
        command.Parameters.Clear();
    
    command.AddWithValue(
        parameterName: "@levelId",
        value: l.Id.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@levelIndex",
        value: l.Index);
    
    command.AddWithValue(
        parameterName: "@levelName",
        value: l.Name is not null
            ? l.Name.TrimDoubleQuotes()
            : DBNull.Value);

        await command.ExecuteNonQueryAsync();
    }

    transaction.Commit();
    
}

command.Transaction = null;
command.Parameters.Clear();

command.CommandText = "INSERT INTO GTFS_PATHWAYS (" +
                            "PathwayId, " +
                            "FromStopId, " +
                            "ToStopId, " +
                            "PathwayMode, " +
                            "IsBidirectional, " +
                            "Length, " +
                            "TraversalTime, " +
                            "StairCount, " +
                            "MaxSlope, " +
                            "MinWidth, " +
                            "SignpostedAs, " +
                            "ReversedSignpostedAs) " +
                      "VALUES (" +
                            "@pathwayId, " +
                            "@fromStopId, " +
                            "@toStopId, " +
                            "@pathwayMode, " +
                            "@isBidirectional, " +
                            "@length, " +
                            "@traversalTime, " +
                            "@stairCount, " +
                            "@maxSlope, " +
                            "@minWidth, " +
                            "@signpostedAs, " +
                            "@reversedSignpostedAs)";

command.Parameters.Clear();

using (var transaction = connection.BeginTransaction())
{
    command.Transaction = transaction;

    foreach (var p in feed.Pathways)
    {
        command.Parameters.Clear();
    
    command.AddWithValue(
        parameterName: "@pathwayId",
        value: p.Id.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@fromStopId",
        value: p.FromStopId.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@toStopId",
        value: p.ToStopId.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@pathwayMode",
        value: p.PathwayMode);
    
    command.AddWithValue(
        parameterName: "@isBidirectional",
        value: p.IsBidirectional);
    
    command.AddWithValue(
        parameterName: "@length",
        value: p.Length is not null
            ? p.Length
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@traversalTime",
        value: p.TraversalTime is not null
            ? p.TraversalTime
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@stairCount",
        value: p.StairCount is not null
            ? p.StairCount
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@maxSlope",
        value: p.MaxSlope is not null
            ? p.MaxSlope.ToString() != string.Empty
                ? p.MaxSlope
                : string.Empty
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@minWidth",
        value: p.MinWidth is not null
            ? p.MinWidth
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@signpostedAs",
        value: p.SignpostedAs is not null
            ? p.SignpostedAs.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@reversedSignpostedAs",
        value: p.ReversedSignpostedAs is not null
            ? p.ReversedSignpostedAs.TrimDoubleQuotes()
            : DBNull.Value);

        await command.ExecuteNonQueryAsync();
    }

    transaction.Commit();
    
}

command.Transaction = null;
command.Parameters.Clear();

command.CommandText = "INSERT INTO GTFS_ROUTES (" +
                            "RouteId, " +
                            "AgencyId, " +
                            "RouteShortName, " +
                            "RouteLongName, " +
                            "RouteDesc, " +
                            "RouteType, " +
                            "RouteUrl, " +
                            "RouteColor, " +
                            "RouteTextColor) " +
                      "VALUES (" +
                            "@routeId, " +
                            "@agencyId, " +
                            "@routeShortName, " +
                            "@routeLongName, " +
                            "@routeDesc, " +
                            "@routeType, " +
                            "@routeUrl, " +
                            "@routeColor, " +
                            "@routeTextColor)";

command.Parameters.Clear();

using (var transaction = connection.BeginTransaction())
{
    command.Transaction = transaction;

    foreach (var r in feed.Routes)
    {
        command.Parameters.Clear();
    
    command.AddWithValue(
        parameterName: "@routeId",
        value: r.Id.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@agencyId",
        value: r.AgencyId is not null
            ? r.AgencyId.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@routeShortName",
        value: r.ShortName is not null
            ? r.ShortName.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@routeLongName",
        value: r.LongName is not null
            ? r.LongName.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@routeDesc",
        value: r.Description is not null
            ? r.Description.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@routeType",
        value: r.Type);
    
    command.AddWithValue(
        parameterName: "@routeUrl",
        value: r.Url is not null
            ? r.Url.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@routeColor",
        value: r.Color is not null
            ? r.Color.ToString() != string.Empty
                ? r.Color
                : string.Empty
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@routeTextColor",
        value: r.TextColor is not null
            ? r.TextColor.ToString() != string.Empty
                ? r.TextColor
                : string.Empty
            : DBNull.Value);

        await command.ExecuteNonQueryAsync();
    }

    transaction.Commit();
    
}

command.Transaction = null;
command.Parameters.Clear();

command.CommandText = "INSERT INTO GTFS_SHAPES (" +
                            "ShapeId, " +
                            "ShapePtLat, " +
                            "ShapePtLon, " +
                            "ShapePtSequence, " +
                            "ShapeDistanceTravelled) " +
                      "VALUES (" +
                            "@shapeId, " +
                            "@shapePtLat, " +
                            "@shapePtLon, " +
                            "@shapePtSequence, " +
                            "@shapeDistanceTravelled)";

command.Parameters.Clear();

using (var transaction = connection.BeginTransaction())
{
    command.Transaction = transaction;

    foreach (var s in feed.Shapes)
    {
        command.Parameters.Clear();
    
    command.AddWithValue(
        parameterName: "@shapeId",
        value: s.Id.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@shapePtLat",
        value: s.Latitude);
    
    command.AddWithValue(
        parameterName: "@shapePtLon",
        value: s.Longitude);
    
    command.AddWithValue(
        parameterName: "@shapePtSequence",
        value: Convert.ToInt32(s.Sequence));
    
    command.AddWithValue(
        parameterName: "@shapeDistanceTravelled",
        value: s.DistanceTravelled is not null
            ? s.DistanceTravelled
            : DBNull.Value);

        await command.ExecuteNonQueryAsync();
    }

    transaction.Commit();
    
}

command.Transaction = null;
command.Parameters.Clear();

command.CommandText = "INSERT INTO GTFS_STOPS (" +
                            "StopId, " +
                            "StopCode, " +
                            "StopName, " +
                            "StopDesc, " +
                            "StopLat, " +
                            "StopLon, " +
                            "ZoneId, " +
                            "StopUrl, " +
                            "LocationType, " +
                            "ParentStation, " +
                            "StopTimezone, " +
                            "WheelchairBoarding, " +
                            "LevelId, " +
                            "PlatformCode) " +
                      "VALUES (" +
                            "@stopId, " +
                            "@stopCode, " +
                            "@stopName, " +
                            "@stopDesc, " +
                            "@stopLat, " +
                            "@stopLon, " +
                            "@zoneId, " +
                            "@stopUrl, " +
                            "@locationType, " +
                            "@parentStation, " +
                            "@stopTimezone, " +
                            "@wheelchairBoarding, " +
                            "@levelId, " +
                            "@platformCode)";

command.Parameters.Clear();

using (var transaction = connection.BeginTransaction())
{
    command.Transaction = transaction;

    foreach (var s in feed.Stops)
    {
        command.Parameters.Clear();
    
    command.AddWithValue(
        parameterName: "@stopId",
        value: s.Id.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@stopCode",
        value: s.Code is not null
            ? s.Code.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@stopName",
        value: s.Name is not null
            ? s.Name.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@stopDesc",
        value: s.Description is not null
            ? s.Description.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@stopLat",
        value: s.Latitude);
    
    command.AddWithValue(
        parameterName: "@stopLon",
        value: s.Longitude);
    
    command.AddWithValue(
        parameterName: "@zoneId",
        value: s.Zone is not null
            ? s.Zone.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@stopUrl",
        value: s.Url is not null
            ? s.Url.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@locationType",
        value: s.LocationType is not null
            ? s.LocationType.ToString() != string.Empty
                ? s.LocationType
                : string.Empty
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@parentStation",
        value: s.ParentStation is not null
            ? s.ParentStation.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@stopTimezone",
        value: s.Timezone is not null
            ? s.Timezone.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@wheelchairBoarding",
        value: s.WheelchairBoarding is not null
            ? s.WheelchairBoarding.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@levelId",
        value: s.LevelId is not null
            ? s.LevelId.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@platformCode",
        value: s.PlatformCode is not null
            ? s.PlatformCode.TrimDoubleQuotes()
            : DBNull.Value);

        await command.ExecuteNonQueryAsync();
    }

    transaction.Commit();
    
}

command.Transaction = null;
command.Parameters.Clear();

command.CommandText = "INSERT INTO GTFS_STOP_TIMES (" +
                            "TripId, " +
                            "ArrivalTime, " +
                            "DepartureTime, " +
                            "StopId, " +
                            "StopSequence, " +
                            "StopHeadsign, " +
                            "PickupType, " +
                            "DropOffType, " +
                            "ShapeDistTravelled, " +
                            "Timepoint) " +
                      "VALUES (" +
                            "@tripId, " +
                            "@arrivalTime, " +
                            "@departureTime, " +
                            "@stopId, " +
                            "@stopSequence, " +
                            "@stopHeadsign, " +
                            "@pickupType, " +
                            "@dropOffType, " +
                            "@shapeDistTravelled, " +
                            "@timepoint)";

command.Parameters.Clear();

using (var transaction = connection.BeginTransaction())
{
    command.Transaction = transaction;

    foreach (var s in feed.StopTimes)
    {
        command.Parameters.Clear();
    
    command.AddWithValue(
        parameterName: "@tripId",
        value: s.TripId.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@arrivalTime",
        value: s.ArrivalTime is not null
            ? s.ArrivalTime.ToString()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@departureTime",
        value: s.DepartureTime is not null
            ? s.DepartureTime.ToString()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@stopId",
        value: s.StopId is not null
            ? s.StopId.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@stopSequence",
        value: s.StopSequence);
    
    command.AddWithValue(
        parameterName: "@stopHeadsign",
        value: s.StopHeadsign is not null
            ? s.StopHeadsign.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@pickupType",
        value: s.PickupType is not null
            ? s.PickupType.ToString() != string.Empty
                ? s.PickupType
                : string.Empty
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@dropOffType",
        value: s.DropOffType is not null
            ? s.DropOffType.ToString() != string.Empty
                ? s.DropOffType
                : string.Empty
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@shapeDistTravelled",
        value: s.ShapeDistTravelled is not null
            ? s.ShapeDistTravelled
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@timepoint",
        value: s.TimepointType.ToString() != string.Empty
            ? s.TimepointType
            : DBNull.Value);

        await command.ExecuteNonQueryAsync();
    }

    transaction.Commit();
    
}

command.Transaction = null;
command.Parameters.Clear();

command.CommandText = "INSERT INTO GTFS_TRANSFERS (" +
                            "FromStopId, " +
                            "ToStopId, " +
                            "TransferType, " +
                            "MinTransferTime) " +
                      "VALUES (" +
                            "@fromStopId, " +
                            "@toStopId, " +
                            "@transferType, " +
                            "@minTransferTime)";

command.Parameters.Clear();

using (var transaction = connection.BeginTransaction())
{
    command.Transaction = transaction;

    foreach (var t in feed.Transfers)
    {
        command.Parameters.Clear();
    
    command.AddWithValue(
        parameterName: "@fromStopId",
        value: t.FromStopId is not null
            ? t.FromStopId.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@toStopId",
        value: t.ToStopId is not null
            ? t.ToStopId.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@transferType",
        value: t.TransferType.ToString() != string.Empty
            ? t.TransferType
            : string.Empty);
    
    command.AddWithValue(
        parameterName: "@minTransferTime",
        value: t.MinimumTransferTime is not null
            ? t.MinimumTransferTime
            : DBNull.Value);

        await command.ExecuteNonQueryAsync();
    }

    transaction.Commit();
    
}

command.Transaction = null;
command.Parameters.Clear();

command.CommandText = "INSERT INTO GTFS_TRIPS (" +
                            "RouteId, " +
                            "ServiceId, " +
                            "TripId, " +
                            "TripHeadsign, " +
                            "TripShortName, " +
                            "DirectionId, " +
                            "BlockId, " +
                            "ShapeId, " +
                            "WheelchairAccessible) " +
                      "VALUES (" +
                            "@routeId, " +
                            "@serviceId, " +
                            "@tripId, " +
                            "@tripHeadsign, " +
                            "@tripShortName, " +
                            "@directionId, " +
                            "@blockId, " +
                            "@shapeId, " +
                            "@wheelchairAccessible)";

command.Parameters.Clear();

using (var transaction = connection.BeginTransaction())
{
    command.Transaction = transaction;

    foreach (var t in feed.Trips)
    {
        command.Parameters.Clear();
    
    command.AddWithValue(
        parameterName: "@routeId",
        value: t.RouteId.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@serviceId",
        value: t.ServiceId.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@tripId",
        value: t.Id.TrimDoubleQuotes());
    
    command.AddWithValue(
        parameterName: "@tripHeadsign",
        value: t.Headsign is not null
            ? t.Headsign.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@tripShortName",
        value: t.ShortName is not null
            ? t.ShortName.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@directionId",
        value: t.Direction is not null
            ? t.Direction
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@blockId",
        value: t.BlockId is not null
            ? t.BlockId.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@shapeId",
        value: t.ShapeId is not null
            ? t.ShapeId.TrimDoubleQuotes()
            : DBNull.Value);
    
    command.AddWithValue(
        parameterName: "@wheelchairAccessible",
        value: t.AccessibilityType is not null
            ? t.AccessibilityType.ToString() != string.Empty
                ? t.AccessibilityType
                : string.Empty
            : DBNull.Value);

        await command.ExecuteNonQueryAsync();
    }

    transaction.Commit();
    
}

command.Transaction = null;
command.Parameters.Clear();

command.CommandText = "CREATE INDEX GTFS_STOP_TIMES_INDEX ON GTFS_STOP_TIMES (" +
                            "TripId, " +
                            "StopId, " +
                            "PickupType, " +
                            "ArrivalTime, " +
                            "DepartureTime, " +
                            "StopSequence, " +
                            "StopHeadsign, " +
                            "DropOffType, " +
                            "ShapeDistTravelled, " +
                            "Timepoint)";

await command.ExecuteNonQueryAsync();

await command.DisposeAsync();
await connection.CloseAsync();