package gui;

import autowebservices.database.DB;
import autowebservices.datapull.SQLPull;
import autowebservices.grammar.JSONLexer;
import autowebservices.grammar.JSONParser;
import javafx.embed.swing.SwingFXUtils;
import javafx.fxml.FXMLLoader;
import javafx.scene.Group;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.json.JSONArray;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Scanner;

/**
 * @author Curtis Dyreson & Arihant Jain
 */
public class Controller {
    public TextField dburi;
    public TextField dbname;
    public PasswordField dbpass;
    public TextField dbuser;
    public TextField dbport;
    public AnchorPane anchorPane;
    public Button connectdb;
    public TextArea jsonschema;
    public TextField pathNumber;
    public TitledPane titledPane;
    public TextArea jsonout;
    public Button generatews;
    private String finalQuery;
    private String newSchema;
    private DB db;
    private File[] filesJpg;

    public void connectDatabase() throws IOException {
        FileWriter fileWriter = new FileWriter("generatedfiles/dbinfo.txt");
        String url = "jdbc:postgresql://" + dburi.getText() + ":" + dbport.getText() + "/"
                + "!" + dbname.getText() + "!" + dbuser.getText() + "!" + dbpass.getText();
        fileWriter.write(url);
        fileWriter.close();
        try {
            db = establishConnection();
            loadApplication();
        } catch (SQLException e) {
            Stage stage = new Stage();
            TextArea text = new TextArea();
            text.setEditable(false);
            text.setText("Unable to establish database connection");
            Scene scene = new Scene(text, 400, 40);
            stage.setScene(scene);
            stage.show();

        }
    }

    private DB establishConnection() throws FileNotFoundException, SQLException {
        File file = new File("generatedfiles/dbinfo.txt");
        Scanner sc = new Scanner(file);
        String[] dbinfo = sc.nextLine().split("!");
        return new DB(dbinfo[0], dbinfo[1], dbinfo[2], dbinfo[3]);
    }

    private void loadApplication() throws IOException {
        FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource("main.fxml"));
        Parent root = fxmlLoader.load();
        Stage stage = Main.getPrimaryStage();
        Scene scene = stage.getScene();
        scene.setRoot(root);
        stage.show();
    }

    public void generatePaths() throws SQLException, IOException {
        db = establishConnection();
        String jschema = jsonschema.getText();
        FileWriter fileWriter = new FileWriter("generatedfiles/schema.json");
        fileWriter.write(jschema);
        fileWriter.close();
        CharStream charStream = CharStreams.fromString(jschema);
        JSONLexer lexer = new JSONLexer(charStream);
        CommonTokenStream ts = new CommonTokenStream(lexer);
        JSONParser parser = new JSONParser(ts);
        parser.json(db);
        generateImages();
    }

    private void generateImages() throws IOException {
//        ProcessBuilder builder = new ProcessBuilder("python3", "creategraphimages.py");
        ProcessBuilder builder = new ProcessBuilder("python", "creategraphimages.py");
        Process p = builder.start();
        try {
            p.waitFor();
        } catch (InterruptedException ignored) {
        }
        showImagesInApp();
    }

    private void showImagesInApp() {
//        File selectedDirectory = new File("images/");
        File selectedDirectory = new File("images\\");
        FilenameFilter filterJpg = (dir, name) -> name.toLowerCase().endsWith(".png");
        filesJpg = selectedDirectory.listFiles(filterJpg);
        if (filesJpg != null) {
            Arrays.sort(filesJpg);
        }
        openTitledPane();
    }

    private void openTitledPane() {
        int numOfJpg = filesJpg.length;
        Image[] images = new Image[numOfJpg];
        BufferedImage[] bufferedImage = new BufferedImage[numOfJpg];
        ImageView[] imageViews = new ImageView[numOfJpg];
        TitledPane[] titledPanes = new TitledPane[numOfJpg];
        for (int i = 0; i < numOfJpg; i++) {
            try {
                File file = filesJpg[i];
                bufferedImage[i] = ImageIO.read(file);
                images[i] = SwingFXUtils.toFXImage(bufferedImage[i], null);
                imageViews[i] = new ImageView();
                imageViews[i].setImage(images[i]);
                imageViews[i].setFitWidth(365);
                imageViews[i].setPreserveRatio(true);
                imageViews[i].setSmooth(true);
                imageViews[i].setCache(true);
                titledPanes[i] = new TitledPane(String.valueOf(i), imageViews[i]);
            } catch (IOException ignored) {
            }
        }
        Accordion accordion = new Accordion();
        accordion.getPanes().addAll(titledPanes);
        titledPane.setContent(accordion);
    }

    public void showWebServices() {
        String paths = pathNumber.getText();
        String[] tempPath = paths.split(" ");
        int[] path = new int[tempPath.length];
        for (int i = 0; i < tempPath.length; i++)
            path[i] = Integer.parseInt(tempPath[i]);
        String[] temp2 = usingBufferedReader("generatedfiles/queries.txt").split("!!!");
        String[] pathQueries = new String[(temp2.length - 1) / 3 + 1];
        int j = 0;
        for (int i = 0; i < temp2.length - 1; i++) {
            if (i % 3 == 0)
                pathQueries[j++] = temp2[i];
        }
        StringBuilder result = new StringBuilder();
        if (path.length > 1) {
            for (int value = 0; value < path.length; value++) {
                int index = pathQueries[value].lastIndexOf("ORDER BY");
                if (result.toString().equals("")) {
                    result.append(pathQueries[value], 0, index);
                } else result.append(" UNION \n").append(pathQueries[value], 0, index);
            }
            result.append(temp2[temp2.length - 1]);
        } else {
            result.append(pathQueries[path[0]]);
        }
        finalQuery = result.toString();
        generateOutput();
    }

    private void generateOutput() {
        try {
            demoOutputGenerator(finalQuery);
            prettifyJson();
        } catch (SQLException | IOException ignored) {
        }
    }

    private void prettifyJson() throws IOException {
        ProcessBuilder builder = new ProcessBuilder("python", "prettyjson.py");
        Process p = builder.start();
        try {
            p.waitFor();
        } catch (InterruptedException ignored) {
        }
        String prettyJson = usingBufferedReader("generatedfiles/demooutput.txt");
        showJson(prettyJson);
    }

    private void showJson(String json) {
        jsonout.setEditable(false);
        jsonout.setText(json);
    }

    private void demoOutputGenerator(String query) throws SQLException, IOException {
        String filePath = "generatedfiles/schema.json";
        db = establishConnection();
        SQLPull sqlPull = new SQLPull();
        JSONArray jsonArray = sqlPull.convertQueryResultToJson(db.executeQuery(query + "LIMIT 100"));
        String[] fillArray = new String[jsonArray.length()];
        for (int i = 0; i < jsonArray.length(); i++) {
            String str = jsonArray.get(i).toString();
            if (jsonArray.get(i).toString().equals("{}")) {
                fillArray[i] = "null";
            } else {
                fillArray[i] = str.split("\":")[1].replace("}", "");
            }
        }
        String finalOut = sqlPull.hydrateJson(filePath, fillArray, sqlPull.getCountForValues(filePath));
        FileWriter fileWriter = new FileWriter("generatedfiles/demooutput.txt");
        fileWriter.write(finalOut);
        fileWriter.close();
        newSchema = sqlPull.generateFillableSchema(filePath).toString();
    }

    private static String usingBufferedReader(String fileName) {
        StringBuilder contentBuilder = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String sCurrentLine;
            while ((sCurrentLine = br.readLine()) != null) {
                contentBuilder.append(sCurrentLine).append("\n");
            }
        } catch (IOException ignored) {
        }
        return contentBuilder.toString();
    }

    public void generateWebServices() {
        File file = new File("generatedfiles/dbinfo.txt");
        Scanner sc = null;
        try {
            sc = new Scanner(file);
        } catch (FileNotFoundException ignored) {
        }
        String[] dbinfo = new String[0];
        if (sc != null) {
            dbinfo = sc.nextLine().split("!");
        }
        String[] temp_host_port = dbinfo[0].split("//")[1].split(":");
        String py_ws = "from flask import Flask\n" +
                "import psycopg2\n" +
                "import json\n" +
                "\n" +
                "app = Flask(__name__)\n" +
                "\n" +
                "host = '" + temp_host_port[0] + "' \n" +
                "database = '" + dbinfo[1] + "' \n" +
                "user = '" + dbinfo[2] + "' \n" +
                "password = '" + dbinfo[3] + "' \n" +
                "port = '" + temp_host_port[1].split("/")[0] + "' \n" +
                "query = '''" + finalQuery + " LIMIT 1000" + "''' \n" +
                "json_schema = '''" + newSchema + "''' \n" +
                "@app.route('/')\n" +
                "def hello_world():\n" +
                "    conn = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)\n" +
                "    cur = conn.cursor()\n" +
                "    cur.execute(query)\n" +
                "    result = cur.fetchall()\n" +
                "    ugly_json = '['\n" +
                "    for row in result:\n" +
                "        temp = json_schema\n" +
                "        for r in row:\n" +
                "            if r is None:\n" +
                "                temp = temp.replace('\"\"', 'null', 1)\n" +
                "            else:\n" +
                "                temp = temp.replace('\"\"', '\"' + str(r) + '\"', 1)\n" +
                "        ugly_json = ugly_json + temp + ','\n" +
                "\n" +
                "    ugly_json = ugly_json[:-1]\n" +
                "    ugly_json = ugly_json + ']'\n" +
                "    parsed = json.loads(ugly_json)\n" +
                "    pretty_json = json.dumps(parsed, indent=2)\n" +
                "    return pretty_json\n" +
                "\n" +
                "\n" +
                "if __name__ == '__main__':\n" +
                "    app.run()\n";
        Group root = new Group();
        Stage stage = new Stage();
        stage.setTitle("Generated Web Service");
        Button save_ws = new Button();
        TextArea textArea = new TextArea();
        textArea.setEditable(false);
        textArea.setMinSize(800, 725);
        textArea.setText(py_ws);
        save_ws.setText("Save file as");
        save_ws.setOnAction(event -> {
            FileChooser fileChooser = new FileChooser();
            FileChooser.ExtensionFilter extFilter = new FileChooser.ExtensionFilter(".py files", "*.PY");
            fileChooser.getExtensionFilters().add(extFilter);
            File file1 = fileChooser.showSaveDialog(stage);
            if (file1 != null) {
                savePyFile(py_ws, file1);
            }
        });
        VBox vBox = new VBox();
        vBox.getChildren().addAll(textArea, save_ws);
        root.getChildren().add(vBox);
        stage.setScene(new Scene(root, 800, 750));
        stage.show();
    }

    private void savePyFile(String content, File file) {
        try {
            FileWriter fileWriter;
            fileWriter = new FileWriter(file.getAbsolutePath() + ".py");
            fileWriter.write(content);
            fileWriter.close();
        } catch (IOException ignored) {
        }
    }
}
