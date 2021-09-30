import download from "download";
import * as fs from "fs";
import { parseFile } from "fast-csv";
import csvToJson from "csvtojson";

const CSV_PATH =
  "https://gist.githubusercontent.com/bobbae/b4eec5b5cb0263e7e3e63a6806d045f2/raw/279b794a834a62dc108fc843a72c94c49361b501/data.csv";
const CSV_NAME = "data.csv";

async function all() {
  if (fs.existsSync("data2.json")) {
    fs.unlink("data2.json", (err) => {
      if (err) {
        console.error(err);
      }
    });
  }
  if (fs.existsSync("formatted.csv")) {
    fs.unlink("formatted.csv", (err) => {
      if (err) {
        console.error(err);
      }
    });
  }
  fs.writeFileSync("data2.json", "");
  fs.writeFileSync("formatted.csv", "");
  await download(CSV_PATH, ".");

  const jsonObject = {};
  let validRows = 0;
  const maxRows = {
    maxIds: [],
    maxProfits: [],
    rows: [],
  };

  const writeStream = fs.createWriteStream("formatted.csv", {
    flags: "a",
  });
  const data2JsonWriteStream = fs.createWriteStream("data2.json", {
    flags: "a",
  });

  parseFile(CSV_NAME)
    .on("error", (error) => console.error(error))
    .on("data", () => {})
    .on("end", (rowCount) => {
      console.log(`TOTAL ROWS: ${rowCount}`);
      parseFile(CSV_NAME)
        .on("error", (error) => console.error(error))
        .on("data", (row) => {
          if (row && Array.isArray(row)) {
            const headerRow = row.includes("Year");

            let firstRow = row;
            if (headerRow) {
              firstRow[3] = "Revenue";
              firstRow[4] = "Profit";

              firstRow.forEach((title) => {
                jsonObject[title] = [];
              });
            }

            if (!isNaN(row[4]) || headerRow) {
              validRows += 1;
              writeStream.write(
                headerRow ? `${firstRow.join(",")}\n` : `${row.join(",")}\n`
              );
            }
          }
        })
        .on("end", async (rowCount) => {
          console.log(`TOTAL VALID PROFIT ROWS: ${validRows}`);
          const jsonArray = await csvToJson().fromFile("formatted.csv");
          const sortedJsonArray = jsonArray.sort(function compare(a, b) {
            if (Number(a.Profit) > Number(b.Profit)) {
              return -1;
            }
            if (Number(a.Profit) < Number(b.Profit)) {
              return 1;
            }
            return 0;
          });

          const topTwenty = [];
          sortedJsonArray.forEach((value, index) => {
            if (index < 20) {
              topTwenty.push(value);
            }
          });
          console.log("TOP TWENTY HIGHEST PROFIT ROWS ", topTwenty);
          parseFile("formatted.csv")
            .on("error", (error) => console.error(error))
            .on("data", (row) => {
              const headerRow = row.includes("Year");
              if (!headerRow) {
                jsonObject["Year"] = [...jsonObject["Year"], row[0]];
                jsonObject["Rank"] = [...jsonObject["Rank"], row[1]];
                jsonObject["Company"] = [...jsonObject["Company"], row[2]];
                jsonObject["Revenue"] = [...jsonObject["Revenue"], row[3]];
                jsonObject["Profit"] = [...jsonObject["Profit"], row[4]];
              }
            })
            .on("end", async (rowCount) => {
              data2JsonWriteStream.write(JSON.stringify(jsonObject));
            });
        });
    });
}

all();

const w = [
  {
    ID: "96",
    Year: "1955",
    Rank: "96",
    Company: "Amstar",
    Revenue: "308.8",
    Profit: "7.6",
  },
  {
    ID: "97",
    Year: "1955",
    Rank: "97",
    Company: "Reynolds Metals",
    Revenue: "306.8",
    Profit: "20.3",
  },
  {
    ID: "98",
    Year: "1955",
    Rank: "98",
    Company: "Morrell (John)",
    Revenue: "306.5",
    Profit: "0.5",
  },
  {
    ID: "99",
    Year: "1955",
    Rank: "99",
    Company: "BP America",
    Revenue: "304.4",
    Profit: "18.5",
  },
];
