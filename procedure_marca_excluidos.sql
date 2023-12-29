CREATE OR REPLACE PROCEDURE update_movimentacao()
LANGUAGE plpgsql
AS $$
DECLARE
    exc_record RECORD;
    mov_record RECORD;
    linhas_atualizadas int;
BEGIN
    -- Loop through records in movimentacao_exc table
    FOR exc_record IN (SELECT id, hash FROM movimentacao_exc where processado = 0) LOOP
        -- Update the first matching record in the movimentacao table
        linhas_atualizadas := 0;
        for mov_record in (select id, hash from movimentacao where hash = exc_record.hash and processado = 0 limit 1) loop
            update movimentacao set processado = -1 where id = mov_record.id;
            GET DIAGNOSTICS linhas_atualizadas := ROW_COUNT;
        END LOOP;
        if linhas_atualizadas > 0 then
            update movimentacao_exc set processado = -1 where id = exc_record.id;
        end if;
    end loop;
END;
$$;
