import * as Transport from "winston-transport";
import {FluentClient, FluentClientOptions} from "./client";
import EventTime from "./event_time";
import * as urlParse from "url-parse";

interface TransportInfo {
  message: string;
  level?: string;
  labels?: Record<string, unknown>;
}

interface TransportOptions {
  host: string;
  labels: Record<string, unknown>;
  handleExceptions?: boolean;
  levels?: unknown;
}

/**
 * A Winston transport for Fluent.
 *
 * @class FluentTransport
 * @extends {Transport}
 */
export class FluentTransport extends Transport {
  // a list o labels.
  private labels: Record<string, unknown> = {};

  // a fluent host.
  private host = "";

  // logger
  private logger: null | FluentClient = null;

  /**
   * Creates an instance of LokiTransport.
   * @param {TransportOptions} options
   * @param {FluentClientOptions} fluentOptions
   * @memberof LokiTransport
   */
  constructor(options: TransportOptions, fluentOptions: FluentClientOptions) {
    super(options);

    this.labels = options.labels;
    this.host = options.host;

    this.initLogger(fluentOptions);
  }

  /**
   * Init logger.
   * @param {FluentClientOptions} fluentOptions
   */
  private initLogger(fluentOptions: FluentClientOptions) {
    const url = urlParse(this.host);
    const defaultOptions = {
      socket: {
        host: url.hostname,
        port: parseInt(url.port, 10),
      },
    };

    console.log({...defaultOptions, ...fluentOptions});

    this.logger = new FluentClient("ld", {...defaultOptions, ...fluentOptions});
  }

  /**
   * An overwrite of winston-transport's log(),
   * which the Winston logging library uses
   * when pushing logs to a transport.
   *
   * @param {TransportInfo} info
   * @param {*} callback
   * @memberof LokiTransport
   */
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  public log(info: TransportInfo, next: () => void = () => {}) {
    setImmediate(() => {
      this.emit("logged", info);
    });

    // Deconstruct the log
    const {labels, level, message} = info;

    try {
      // send message
      this.send(message, {
        level: level,
        ...labels,
        ...this.labels,
      });
    } catch (err) {
      this.emit("warn", err);
    }

    // next transport
    next();
  }

  /**
   * Creats an entry.
   * @param {String} message - an log message
   * @param {Record} labels - an object with lables.
   */
  private createEntry = (message: string, labels = {}) => {
    return {
      ...labels,
      message,
    };
  };

  /**
   * Send an entry to loki
   * @param {String} message - an log message
   * @param {Record} labels - an object with lables.
   */
  public send = (message: string, labels = {}) => {
    const eventTime = new EventTime(
      parseInt(String(Date.now() / 1000)),
      process.hrtime()[1]
    );

    this.logger?.emit(this.createEntry(message, labels), eventTime);
  };

  /**
   * Close the transport.
   */
  public async close() {
    this.logger?.syncFlush();

    await this.logger?.disconnect();
  }
}
