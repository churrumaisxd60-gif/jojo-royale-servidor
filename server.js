// Servidor en línea de JoJo Royale (Render.com u otro host con Node 18+).
//
// Un solo WebSocket por jugador. Los mensajes de TEXTO son JSON de control;
// los BINARIOS son paquetes de la partida y se reenvían tal cual al rival de
// la sala (el anfitrión simula la batalla, como en la red local).
//
// Cliente -> servidor (campo "t"):
//   hola        {id, nombre, tag, estado, amigos:[ids], v}   registrarse
//   estado      {estado}                                     menu|grupo|partida
//   amigos      {ids}                                         a quién seguir
//   buscar      {riot:"Nombre#TAG", ref}                     -> buscado
//   msg         {a:id, datos}                                 -> msg al destinatario
//   sala_crear  {}                                            -> sala_creada {codigo}
//   sala_unir   {codigo}                                      -> sala_lista | sala_error
//   rapida      {} / rapida_cancelar {}                       emparejamiento al azar
//   sala_salir  {}                                            -> sala_fin al rival
//   ping        {}                                            -> pong
// Servidor -> cliente: bienvenida, presencia {id,nombre,tag,estado}, fuera {id},
//   buscado, msg {de,nombre,tag,datos}, no_conectado {a}, sala_creada,
//   sala_lista {anfitrion, codigo, rival:{id,nombre,tag}}, sala_error {motivo},
//   sala_fin {motivo}, rapida_esperando, pong.
//
// Nada se guarda en disco (el plan gratis de Render no tiene disco
// persistente): las amistades las guarda cada juego en su perfil; el
// servidor solo sabe quién está conectado ahora.

const http = require("http");
const { WebSocketServer } = require("ws");

const PUERTO = process.env.PORT || 8080;
const VERSION = 1;
const MAX_JUGADORES = 2000;
const MAX_MENSAJE = 64 * 1024;
const ALFABETO_CODIGO = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

/** id -> conexión */
const jugadores = new Map();
/** codigo -> {anfitrion, invitado} */
const salas = new Map();
/** conexiones esperando batalla rápida */
let colaRapida = [];

const servidor = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end(`JoJo Royale en línea OK · ${jugadores.size} conectados · ${salas.size} salas\n`);
});

const wss = new WebSocketServer({ server: servidor, maxPayload: MAX_MENSAJE });

function enviar(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function publico(ws) {
  return { id: ws.id, nombre: ws.nombre, tag: ws.tag, estado: ws.estado };
}

/** Avisa a quien sigue a `ws` (lo tiene de amigo) de su presencia. */
function anunciar(ws, fuera = false) {
  for (const otro of jugadores.values()) {
    if (otro !== ws && otro.amigos.has(ws.id)) {
      enviar(otro, fuera ? { t: "fuera", id: ws.id } : { t: "presencia", ...publico(ws) });
    }
  }
}

function texto(v, max) {
  return String(v ?? "").slice(0, max);
}

function codigoNuevo() {
  for (let intento = 0; intento < 100; intento++) {
    let c = "";
    for (let i = 0; i < 5; i++) c += ALFABETO_CODIGO[Math.floor(Math.random() * ALFABETO_CODIGO.length)];
    if (!salas.has(c)) return c;
  }
  return null;
}

function salirDeSala(ws, motivo) {
  colaRapida = colaRapida.filter((c) => c !== ws);
  const codigo = ws.sala;
  if (!codigo) return;
  ws.sala = null;
  const sala = salas.get(codigo);
  if (!sala) return;
  salas.delete(codigo);
  const rival = sala.anfitrion === ws ? sala.invitado : sala.anfitrion;
  if (rival) {
    rival.sala = null;
    enviar(rival, { t: "sala_fin", motivo });
  }
}

function emparejar(anfitrion, invitado, codigo) {
  const sala = { anfitrion, invitado, codigo };
  salas.set(codigo, sala);
  anfitrion.sala = codigo;
  invitado.sala = codigo;
  enviar(anfitrion, { t: "sala_lista", anfitrion: true, codigo, rival: publico(invitado) });
  enviar(invitado, { t: "sala_lista", anfitrion: false, codigo, rival: publico(anfitrion) });
}

function rivalDe(ws) {
  const sala = ws.sala && salas.get(ws.sala);
  if (!sala || !sala.invitado) return null;
  return sala.anfitrion === ws ? sala.invitado : sala.anfitrion;
}

wss.on("connection", (ws) => {
  ws.id = null;
  ws.amigos = new Set();
  ws.estado = "menu";
  ws.sala = null;
  ws.vivo = true;
  ws.on("pong", () => (ws.vivo = true));

  ws.on("message", (datos, binario) => {
    ws.vivo = true;
    if (binario) {
      // Paquete de la partida: directo al rival.
      const rival = rivalDe(ws);
      if (rival && rival.readyState === 1) rival.send(datos, { binary: true });
      return;
    }
    let m;
    try {
      m = JSON.parse(datos.toString());
    } catch {
      return;
    }
    if (!m || typeof m !== "object") return;
    if (m.t !== "hola" && !ws.id) return;
    switch (m.t) {
      case "hola": {
        const id = texto(m.id, 32);
        if (!/^[0-9a-f]{16}$/.test(id)) return ws.close(4000, "id inválido");
        if (jugadores.size >= MAX_JUGADORES && !jugadores.has(id)) return ws.close(4001, "servidor lleno");
        // La misma persona conectada dos veces: gana la última.
        const viejo = jugadores.get(id);
        if (viejo && viejo !== ws) {
          salirDeSala(viejo, "Te conectaste desde otro sitio");
          viejo.id = null;
          viejo.close(4002, "sesión reemplazada");
        }
        ws.id = id;
        ws.nombre = texto(m.nombre, 16);
        ws.tag = texto(m.tag, 5).toUpperCase();
        ws.estado = texto(m.estado || "menu", 12);
        ws.amigos = new Set((Array.isArray(m.amigos) ? m.amigos : []).slice(0, 300).map((x) => texto(x, 32)));
        jugadores.set(id, ws);
        enviar(ws, { t: "bienvenida", v: VERSION, conectados: jugadores.size });
        for (const a of ws.amigos) {
          const amigo = jugadores.get(a);
          if (amigo) enviar(ws, { t: "presencia", ...publico(amigo) });
        }
        anunciar(ws);
        break;
      }
      case "estado":
        ws.estado = texto(m.estado, 12);
        if (m.nombre) ws.nombre = texto(m.nombre, 16);
        if (m.tag) ws.tag = texto(m.tag, 5).toUpperCase();
        anunciar(ws);
        break;
      case "amigos":
        ws.amigos = new Set((Array.isArray(m.ids) ? m.ids : []).slice(0, 300).map((x) => texto(x, 32)));
        for (const a of ws.amigos) {
          const amigo = jugadores.get(a);
          if (amigo) enviar(ws, { t: "presencia", ...publico(amigo) });
        }
        break;
      case "buscar": {
        const riot = texto(m.riot, 40);
        const i = riot.lastIndexOf("#");
        let hallado = null;
        if (i > 0) {
          const nombre = riot.slice(0, i).trim().toLowerCase();
          const tag = riot.slice(i + 1).trim().toUpperCase();
          for (const j of jugadores.values()) {
            if (j.id && j.nombre.toLowerCase() === nombre && j.tag === tag) hallado = j;
          }
        }
        enviar(ws, hallado ? { t: "buscado", ref: m.ref, ok: true, ...publico(hallado) } : { t: "buscado", ref: m.ref, ok: false });
        break;
      }
      case "msg": {
        const destino = jugadores.get(texto(m.a, 32));
        if (!destino) return enviar(ws, { t: "no_conectado", a: m.a });
        enviar(destino, { t: "msg", de: ws.id, nombre: ws.nombre, tag: ws.tag, datos: m.datos });
        break;
      }
      case "sala_crear": {
        salirDeSala(ws, "El rival salió");
        const codigo = codigoNuevo();
        if (!codigo) return enviar(ws, { t: "sala_error", motivo: "Inténtalo de nuevo" });
        salas.set(codigo, { anfitrion: ws, invitado: null, codigo });
        ws.sala = codigo;
        enviar(ws, { t: "sala_creada", codigo });
        break;
      }
      case "sala_unir": {
        const codigo = texto(m.codigo, 8).toUpperCase().trim();
        const sala = salas.get(codigo);
        if (!sala) return enviar(ws, { t: "sala_error", motivo: "No existe la sala " + codigo });
        if (sala.invitado) return enviar(ws, { t: "sala_error", motivo: "La sala ya está llena" });
        if (sala.anfitrion === ws) return enviar(ws, { t: "sala_error", motivo: "Es tu propia sala" });
        salirDeSala(ws, "El rival salió");
        salas.delete(codigo);
        emparejar(sala.anfitrion, ws, codigo);
        break;
      }
      case "rapida": {
        salirDeSala(ws, "El rival salió");
        colaRapida = colaRapida.filter((c) => c.readyState === 1 && c !== ws && !c.sala);
        const rival = colaRapida.shift();
        if (rival) {
          const codigo = codigoNuevo();
          emparejar(rival, ws, codigo);
        } else {
          colaRapida.push(ws);
          enviar(ws, { t: "rapida_esperando" });
        }
        break;
      }
      case "rapida_cancelar":
        colaRapida = colaRapida.filter((c) => c !== ws);
        break;
      case "sala_salir":
        salirDeSala(ws, texto(m.motivo || "El rival salió", 60));
        break;
      case "ping":
        enviar(ws, { t: "pong" });
        break;
    }
  });

  ws.on("close", () => {
    salirDeSala(ws, "El rival se desconectó");
    if (ws.id && jugadores.get(ws.id) === ws) {
      jugadores.delete(ws.id);
      anunciar(ws, true);
    }
  });
  ws.on("error", () => {});
});

// Conexiones colgadas (el móvil perdió la red): fuera a los 60 s.
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.vivo) {
      ws.terminate();
      continue;
    }
    ws.vivo = false;
    ws.ping();
  }
}, 30000);

servidor.listen(PUERTO, () => console.log(`JoJo Royale en línea escuchando en ${PUERTO}`));
