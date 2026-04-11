import path from "path";
import { fileURLToPath } from "url";
import HtmlWebpackPlugin from "html-webpack-plugin";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const config = {
  entry: "./src/ui/src/main.tsx",
  output: {
    path: path.resolve(__dirname, "dist/ui"),
    filename: "assets/[name].[contenthash:8].js",
    clean: true,
    publicPath: "/",
  },
  resolve: {
    extensions: [".tsx", ".ts", ".js", ".jsx"],
    extensionAlias: {
      ".js": [".tsx", ".ts", ".js"],
    },
  },
  module: {
    rules: [
      {
        test: /\.(ts|tsx)$/,
        exclude: /node_modules/,
        use: {
          loader: "babel-loader",
          options: {
            presets: [
              ["@babel/preset-env", { targets: "defaults" }],
              ["@babel/preset-react", { runtime: "automatic" }],
              "@babel/preset-typescript",
            ],
          },
        },
      },
      {
        test: /\.css$/,
        use: ["style-loader", "css-loader", "postcss-loader"],
      },
    ],
  },
  plugins: [
    new HtmlWebpackPlugin({
      template: "./src/ui/index.html",
    }),
  ],
  devServer: {
    port: 5173,
    hot: true,
    historyApiFallback: true,
    proxy: [
      { context: ["/api"], target: "http://localhost:8080", changeOrigin: true },
      { context: ["/health"], target: "http://localhost:8080", changeOrigin: true },
    ],
  },
};

export default config;
