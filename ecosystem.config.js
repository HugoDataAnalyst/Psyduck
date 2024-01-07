module.exports = {
  apps: [
    {
      name: "API Server",
      script: "./start_api.py",
      interpreter: "python3.10",
      watch: true,
      autorestart: true,
      restart_delay: 10,
      max_memory_restart: "200M"
    },
    {
      name: "Celery Worker",
      script: "./celery_worker.py",
      interpreter: "python3.10",
      watch: true,
      autorestart: true,
      restart_delay: 10
    },
    {
      name: "Webhook Receiver",
      script: "./start_webhookparser.py",
      interpreter: "python3.10",
      watch: true,
      autorestart: true,
      restart_delay: 10
    }
  ]
};
